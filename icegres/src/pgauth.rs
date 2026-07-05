//! File-based SCRAM-SHA-256 authentication for `icegres serve` (`--auth-file`,
//! env `ICEGRES_AUTH_FILE`) — parity probe A6.
//!
//! # File format
//!
//! One `user:password` pair per line. Blank lines and lines starting with `#`
//! are ignored. The username must not contain `:` (the first `:` splits the
//! line); the password may. Duplicate usernames are a hard startup error.
//!
//! ```text
//! # ops team
//! app_reader:s3cret-pw
//! app_writer:another:pw:with:colons
//! ```
//!
//! # Security model (explicit, default-safe)
//!
//! * **Wire**: authentication runs SCRAM-SHA-256 (RFC 5802 / RFC 7677) via
//!   pgwire's SASL handler — the cleartext password never crosses the wire,
//!   even without TLS. Clients that cannot do SCRAM (ancient libpq) are
//!   rejected rather than silently downgraded to a weaker exchange.
//! * **At rest in memory**: the server keeps only the SCRAM salted hash
//!   (`Hi(password, salt, 4096)` with a random per-user, per-boot 16-byte
//!   salt from `/dev/urandom`), never the cleartext.
//! * **At rest on disk**: the auth file itself holds cleartext passwords —
//!   protect it like `.pgpass` (`chmod 600`). This is the documented
//!   trade-off for a dependency-free operator format.
//! * **No auth file**: the server keeps the historical permissive behavior
//!   (any user/password accepted) and logs a startup WARN saying so; nothing
//!   silently starts enforcing.
//! * Wrong password, unknown user, and not-permitted mechanisms are all
//!   rejected with Postgres error 28P01 semantics.

use std::collections::HashMap;
use std::fmt;
use std::io::Read as _;
use std::path::Path;

use anyhow::{bail, Context as _, Result};
use async_trait::async_trait;
use datafusion_postgres::pgwire::api::auth::sasl::scram::gen_salted_password;
use datafusion_postgres::pgwire::api::auth::{AuthSource, LoginInfo, Password};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// SCRAM iteration count. 4096 is the RFC 7677 minimum and what pgwire's
/// server-first message advertises by default; the stored hash below MUST be
/// computed with the same number.
pub const SCRAM_ITERATIONS: usize = 4096;
const SALT_LEN: usize = 16;

/// Per-user SCRAM verifier: random salt + `Hi(password, salt, 4096)`.
struct ScramEntry {
    salt: Vec<u8>,
    salted_password: Vec<u8>,
}

/// pgwire `AuthSource` backed by a `user:password` file, holding only SCRAM
/// salted hashes after load.
pub struct FileAuthSource {
    users: HashMap<String, ScramEntry>,
}

// Manual Debug: never print salts/hashes into logs.
impl fmt::Debug for FileAuthSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileAuthSource")
            .field("users", &self.users.len())
            .finish()
    }
}

impl FileAuthSource {
    /// Load and validate an auth file. Errors are loud and happen at startup,
    /// never at connection time.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read auth file {}", path.display()))?;
        let pairs = parse_auth_file(&content)
            .with_context(|| format!("invalid auth file {}", path.display()))?;
        if pairs.is_empty() {
            bail!(
                "auth file {} contains no credentials — refusing to start with an \
                 empty user store (remove --auth-file for permissive mode)",
                path.display()
            );
        }
        let mut users = HashMap::with_capacity(pairs.len());
        for (user, password) in pairs {
            let salt = random_salt()?;
            let salted_password = gen_salted_password(&password, &salt, SCRAM_ITERATIONS);
            users.insert(
                user,
                ScramEntry {
                    salt: salt.to_vec(),
                    salted_password,
                },
            );
        }
        Ok(FileAuthSource { users })
    }

    /// Number of loaded users (for the startup log line).
    pub fn user_count(&self) -> usize {
        self.users.len()
    }
}

/// Parse `user:password` lines; `#` comments and blank lines are skipped.
fn parse_auth_file(content: &str) -> Result<Vec<(String, String)>> {
    let mut out: Vec<(String, String)> = Vec::new();
    for (idx, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((user, password)) = line.split_once(':') else {
            bail!("line {}: expected 'user:password'", idx + 1);
        };
        if user.is_empty() {
            bail!("line {}: empty username", idx + 1);
        }
        if password.is_empty() {
            bail!("line {}: empty password for user '{user}'", idx + 1);
        }
        if out.iter().any(|(u, _)| u == user) {
            bail!("line {}: duplicate user '{user}'", idx + 1);
        }
        out.push((user.to_string(), password.to_string()));
    }
    Ok(out)
}

fn random_salt() -> Result<[u8; SALT_LEN]> {
    let mut salt = [0u8; SALT_LEN];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut salt))
        .context("failed to read random salt from /dev/urandom")?;
    Ok(salt)
}

fn auth_failed(username: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_string(),
        "28P01".to_string(), // invalid_password
        format!("password authentication failed for user \"{username}\""),
    )))
}

#[async_trait]
impl AuthSource for FileAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("");
        match self.users.get(username) {
            Some(entry) => Ok(Password::new(
                Some(entry.salt.clone()),
                entry.salted_password.clone(),
            )),
            // Same error shape as a wrong password: do not leak which
            // usernames exist.
            None => Err(auth_failed(username)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_users_comments_and_colons_in_password() {
        let pairs = parse_auth_file("# comment\n\nalice:pw1\nbob:p:w:2\n").unwrap();
        assert_eq!(
            pairs,
            vec![
                ("alice".to_string(), "pw1".to_string()),
                ("bob".to_string(), "p:w:2".to_string()),
            ]
        );
    }

    #[test]
    fn rejects_malformed_lines() {
        assert!(parse_auth_file("no-colon-here\n").is_err());
        assert!(parse_auth_file(":emptyuser\n").err().is_some());
        assert!(parse_auth_file("user:\n").err().is_some());
        assert!(parse_auth_file("dup:a\ndup:b\n").err().is_some());
    }

    #[test]
    fn stores_scram_hash_not_cleartext() {
        let dir = std::env::temp_dir().join(format!("icegres-authtest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("auth.conf");
        std::fs::write(&path, "alice:topsecret\n").unwrap();
        let src = FileAuthSource::load(&path).unwrap();
        assert_eq!(src.user_count(), 1);
        let entry = src.users.get("alice").unwrap();
        assert_eq!(entry.salt.len(), SALT_LEN);
        // The stored bytes are the salted hash, not the password.
        assert_ne!(entry.salted_password, b"topsecret".to_vec());
        assert_eq!(
            entry.salted_password,
            gen_salted_password("topsecret", &entry.salt, SCRAM_ITERATIONS)
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_auth_file_is_a_startup_error() {
        let dir = std::env::temp_dir().join(format!("icegres-authtest2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("auth.conf");
        std::fs::write(&path, "# only comments\n\n").unwrap();
        assert!(FileAuthSource::load(&path).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
