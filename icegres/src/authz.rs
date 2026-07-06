//! Relationship-based access control (ReBAC) modelled on Lakekeeper's
//! authorization model, enforced on every SQL statement over the wire.
//!
//! # Model (mirrors `lakekeeper-authz-openfga`)
//!
//! Entities form a hierarchy — `warehouse → namespace → table` — and a grant
//! at a higher level is inherited by every descendant (a `read` grant on the
//! `demo` namespace lets the principal read every table in `demo`; an `own`
//! grant on the warehouse grants everything). Relations are ordered by
//! strength, matching Lakekeeper's `TableRelation` semantics:
//!
//! * `Own` (Ownership) ⊇ everything — read, write, drop, and grant.
//! * `Write` (CanWriteData) ⊇ `Read` — INSERT / UPDATE / DELETE and SELECT.
//! * `Read` (CanReadData) — SELECT / COPY … TO.
//! * `Drop` (CanDrop) — DROP TABLE.
//!
//! Principals are users (from `--auth-file`) or roles; a user inherits every
//! grant of every role it belongs to (membership is transitive).
//!
//! # Enforcement
//!
//! [`AuthzHook`] runs first in the query-hook chain (and the Flight SQL path
//! calls [`Authorizer::authorize_sql`] directly). Each statement is mapped to
//! the set of (action, table) checks it requires; a denied check aborts the
//! statement with SQLSTATE `42501` (insufficient_privilege). `pg_catalog` /
//! `information_schema` reads, `SET`/`SHOW`, and transaction-control
//! statements are session/metadata operations and are always allowed — the
//! same split Lakekeeper draws between catalog data actions and metadata.
//!
//! # Backend seam
//!
//! Enforcement goes through the [`Authorizer`] trait. [`FileAuthorizer`] is
//! the native ReBAC backend (policy file). A future `OpenFgaAuthorizer` that
//! delegates to Lakekeeper's OpenFGA can implement the same trait without
//! touching the enforcement points.

// In a pure open-source build (`--no-default-features`) the ReBAC model, hook,
// and SQL→action mapping are compiled but dormant — nothing constructs the
// authorizer because the `FileAuthorizer` backend is the managed add-on. The
// seam is intentionally present; silence dead-code lints for that config only.
#![cfg_attr(not(feature = "managed"), allow(dead_code, unused_imports))]

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::{CopySource, Statement, TableObject};
use datafusion_postgres::pgwire::api::{ClientInfo, METADATA_USER};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// A relation a principal can hold on an entity, strongest first. `implies`
/// encodes Lakekeeper's relation strength ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Relation {
    /// Ownership — implies every other relation (read, write, drop, grant).
    Own,
    /// CanWriteData — implies Read.
    Write,
    /// CanReadData.
    Read,
    /// CanDrop.
    Drop,
}

impl Relation {
    /// Does holding `self` satisfy a requirement for `needed`?
    fn implies(self, needed: Relation) -> bool {
        use Relation::*;
        match self {
            Own => true,
            Write => matches!(needed, Write | Read),
            Read => matches!(needed, Read),
            Drop => matches!(needed, Drop),
        }
    }

    fn parse(s: &str) -> Result<Relation> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "own" | "ownership" => Relation::Own,
            "write" | "canwritedata" | "modify" => Relation::Write,
            "read" | "canreaddata" | "select" => Relation::Read,
            "drop" | "candrop" => Relation::Drop,
            other => bail!("unknown relation '{other}' (expected own|write|read|drop)"),
        })
    }
}

/// The data-plane action a SQL statement performs on a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// SELECT / COPY … TO STDOUT.
    ReadData,
    /// INSERT / UPDATE / DELETE / COPY … FROM.
    WriteData,
    /// DROP TABLE.
    DropTable,
}

impl Action {
    fn required(self) -> Relation {
        match self {
            Action::ReadData => Relation::Read,
            Action::WriteData => Relation::Write,
            Action::DropTable => Relation::Drop,
        }
    }
}

/// An entity in the `warehouse → namespace → table` hierarchy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Entity {
    Warehouse,
    Namespace(String),
    Table(String, String),
}

impl Entity {
    /// The entity plus its ancestors, nearest first: a table yields
    /// `[table, namespace, warehouse]` so a grant on any level is honoured.
    fn self_and_ancestors(&self) -> Vec<Entity> {
        match self {
            Entity::Warehouse => vec![Entity::Warehouse],
            Entity::Namespace(ns) => {
                vec![Entity::Namespace(ns.clone()), Entity::Warehouse]
            }
            Entity::Table(ns, t) => vec![
                Entity::Table(ns.clone(), t.clone()),
                Entity::Namespace(ns.clone()),
                Entity::Warehouse,
            ],
        }
    }

    /// Parse a policy-file entity token: `lakehouse`/`*`/`warehouse` →
    /// warehouse; `demo` → namespace; `demo.trips` → table.
    fn parse(token: &str) -> Result<Entity> {
        if token == "*" || token.eq_ignore_ascii_case("warehouse") {
            return Ok(Entity::Warehouse);
        }
        match token.split_once('.') {
            Some((ns, t)) if !ns.is_empty() && !t.is_empty() => {
                Ok(Entity::Table(ns.to_string(), t.to_string()))
            }
            _ => Ok(Entity::Namespace(token.to_string())),
        }
    }
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Entity::Warehouse => write!(f, "<warehouse>"),
            Entity::Namespace(ns) => write!(f, "{ns}"),
            Entity::Table(ns, t) => write!(f, "{ns}.{t}"),
        }
    }
}

/// A table reference in a statement, resolved against the default namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub namespace: String,
    pub table: String,
}

impl TableRef {
    fn entity(&self) -> Entity {
        Entity::Table(self.namespace.clone(), self.table.clone())
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.table)
    }
}

/// Decision returned by an [`Authorizer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Allow,
    /// Denied — carries the required action and target for the error message.
    Deny {
        action: Action,
        target: TableRef,
    },
}

/// Pluggable authorization backend. The native [`FileAuthorizer`] and a future
/// OpenFGA-delegating backend both implement this.
pub trait Authorizer: Send + Sync {
    /// Check a single (principal, action, table) triple.
    fn check(&self, principal: &str, action: Action, target: &TableRef) -> Decision;

    /// Authorize a whole SQL statement: map it to its required checks and deny
    /// on the first failure. `default_namespace` resolves unqualified tables.
    fn authorize_sql(
        &self,
        principal: &str,
        stmt: &Statement,
        default_namespace: &str,
    ) -> Decision {
        for (action, target) in required_checks(stmt, default_namespace) {
            let d = self.check(principal, action, &target);
            if d != Decision::Allow {
                return d;
            }
        }
        Decision::Allow
    }
}

/// Native ReBAC authorizer backed by a policy file — the **managed add-on**
/// authorization backend (behind the `managed` cargo feature). The trait,
/// enforcement hook, model, and SQL→action mapping above are open-source core;
/// this policy engine is the paid layer.
#[cfg(feature = "managed")]
pub struct FileAuthorizer {
    /// principal -> set of (relation, entity) grants held directly.
    grants: HashMap<String, HashSet<(Relation, Entity)>>,
    /// user -> roles it belongs to (already flattened transitively).
    memberships: HashMap<String, HashSet<String>>,
}

#[cfg(feature = "managed")]
impl FileAuthorizer {
    /// Parse a policy file. Lines (comments `#`, blank lines ignored):
    ///
    /// ```text
    /// role analyst              # declare a role (optional; grant implies it)
    /// member alice analyst      # alice inherits analyst's grants
    /// grant analyst read demo   # <principal> <relation> <entity>
    /// grant admin  own   *       # warehouse owner
    /// ```
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading authz file {}", path.display()))?;
        Self::parse(&text)
    }

    fn parse(text: &str) -> Result<Self> {
        let mut grants: HashMap<String, HashSet<(Relation, Entity)>> = HashMap::new();
        let mut direct_members: HashMap<String, HashSet<String>> = HashMap::new();

        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let tok: Vec<&str> = line.split_whitespace().collect();
            let ctx = || format!("authz file line {}", lineno + 1);
            match tok.as_slice() {
                ["role", _name] => { /* declaration only; grants define roles too */ }
                ["member", user, role] => {
                    direct_members
                        .entry((*user).to_string())
                        .or_default()
                        .insert((*role).to_string());
                }
                ["grant", principal, relation, entity] => {
                    let rel = Relation::parse(relation).with_context(ctx)?;
                    let ent = Entity::parse(entity).with_context(ctx)?;
                    grants
                        .entry((*principal).to_string())
                        .or_default()
                        .insert((rel, ent));
                }
                _ => bail!(
                    "{}: expected 'role <name>' | 'member <user> <role>' | \
                     'grant <principal> <relation> <entity>', got: {line}",
                    ctx()
                ),
            }
        }

        // Flatten role membership transitively so check() is a plain lookup.
        let memberships = flatten_memberships(&direct_members);
        Ok(FileAuthorizer {
            grants,
            memberships,
        })
    }

    /// Every principal whose grants apply to `user`: the user plus all roles it
    /// belongs to (transitively).
    fn principals_of(&self, user: &str) -> Vec<String> {
        let mut out = vec![user.to_string()];
        if let Some(roles) = self.memberships.get(user) {
            out.extend(roles.iter().cloned());
        }
        out
    }

    /// Number of principals with at least one grant (for startup logging).
    pub fn grant_count(&self) -> usize {
        self.grants.values().map(HashSet::len).sum()
    }
}

#[cfg(feature = "managed")]
impl Authorizer for FileAuthorizer {
    fn check(&self, principal: &str, action: Action, target: &TableRef) -> Decision {
        let needed = action.required();
        let entities = target.entity().self_and_ancestors();
        for p in self.principals_of(principal) {
            if let Some(held) = self.grants.get(&p) {
                for (rel, ent) in held {
                    if rel.implies(needed) && entities.contains(ent) {
                        return Decision::Allow;
                    }
                }
            }
        }
        Decision::Deny {
            action,
            target: target.clone(),
        }
    }
}

#[cfg(feature = "managed")]
fn flatten_memberships(
    direct: &HashMap<String, HashSet<String>>,
) -> HashMap<String, HashSet<String>> {
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    for user in direct.keys() {
        let mut seen: HashSet<String> = HashSet::new();
        let mut stack: Vec<String> = direct.get(user).into_iter().flatten().cloned().collect();
        while let Some(role) = stack.pop() {
            if seen.insert(role.clone()) {
                if let Some(parents) = direct.get(&role) {
                    stack.extend(parents.iter().cloned());
                }
            }
        }
        out.insert(user.clone(), seen);
    }
    out
}

/// True for schemas that carry catalog/session metadata rather than user data;
/// reads against them are always allowed (Lakekeeper's metadata split).
fn is_system_schema(ns: &str) -> bool {
    matches!(
        ns.to_ascii_lowercase().as_str(),
        "pg_catalog" | "information_schema" | "pg_temp" | "pg_toast"
    )
}

/// Map a statement to the (action, table) checks it requires. Session and
/// metadata statements (SET/SHOW/BEGIN/COMMIT, pg_catalog reads, …) yield no
/// checks and are therefore allowed.
pub fn required_checks(stmt: &Statement, default_ns: &str) -> Vec<(Action, TableRef)> {
    let mut checks = Vec::new();
    match stmt {
        Statement::Query(q) => {
            collect_query_reads(q, default_ns, &mut checks);
        }
        Statement::Insert(insert) => {
            if let TableObject::TableName(name) = &insert.table {
                if let Some(t) = object_name_to_ref(&name.to_string(), default_ns) {
                    checks.push((Action::WriteData, t));
                }
            }
            if let Some(src) = &insert.source {
                collect_query_reads(src, default_ns, &mut checks);
            }
        }
        Statement::Update {
            table, selection, ..
        } => {
            if let Some(t) = table_factor_to_ref(&table.relation, default_ns) {
                checks.push((Action::WriteData, t));
            }
            let _ = selection; // subquery predicates: conservative, not read-gated
        }
        Statement::Delete(del) => {
            for name in &del.tables {
                if let Some(t) = object_name_to_ref(&name.to_string(), default_ns) {
                    checks.push((Action::WriteData, t));
                }
            }
            let froms = match &del.from {
                sqlparser::ast::FromTable::WithFromKeyword(f)
                | sqlparser::ast::FromTable::WithoutKeyword(f) => f,
            };
            for twj in froms {
                if let Some(t) = table_factor_to_ref(&twj.relation, default_ns) {
                    checks.push((Action::WriteData, t));
                }
            }
        }
        Statement::Copy {
            source, to, target, ..
        } => {
            let action = if *to {
                Action::ReadData
            } else {
                Action::WriteData
            };
            match source {
                CopySource::Table { table_name, .. } => {
                    if let Some(t) = object_name_to_ref(&table_name.to_string(), default_ns) {
                        checks.push((action, t));
                    }
                }
                CopySource::Query(q) => collect_query_reads(q, default_ns, &mut checks),
            }
            let _ = target; // STDIN/STDOUT/file target does not add a table
        }
        Statement::Drop { names, .. } => {
            for name in names {
                if let Some(t) = object_name_to_ref(&name.to_string(), default_ns) {
                    checks.push((Action::DropTable, t));
                }
            }
        }
        // SET / SHOW / BEGIN / COMMIT / ROLLBACK / CREATE / EXPLAIN / … are
        // session or metadata operations: no data-plane check.
        _ => {}
    }
    // Drop metadata-schema reads (pg_catalog / information_schema).
    checks.retain(|(_, t)| !is_system_schema(&t.namespace));
    checks
}

/// Recursively collect ReadData checks for every base table referenced by a
/// query (FROM, JOINs, subqueries, CTEs, set operations).
fn collect_query_reads(
    query: &sqlparser::ast::Query,
    default_ns: &str,
    out: &mut Vec<(Action, TableRef)>,
) {
    use sqlparser::ast::visit_relations;
    // visit_relations walks every ObjectName used as a table relation,
    // including inside JOINs, subqueries, CTEs and set operations.
    let _ = visit_relations(query, |name| {
        if let Some(t) = object_name_to_ref(&name.to_string(), default_ns) {
            out.push((Action::ReadData, t));
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
}

fn table_factor_to_ref(factor: &sqlparser::ast::TableFactor, default_ns: &str) -> Option<TableRef> {
    if let sqlparser::ast::TableFactor::Table { name, .. } = factor {
        object_name_to_ref(&name.to_string(), default_ns)
    } else {
        None
    }
}

/// Resolve a possibly-qualified object name (`trips`, `demo.trips`,
/// `icegres.demo.trips`) to a `namespace.table`, defaulting the namespace.
/// Strips the leading catalog component and quotes; metadata-table suffixes
/// like `trips$snapshots` keep the base table for the check.
fn object_name_to_ref(name: &str, default_ns: &str) -> Option<TableRef> {
    let clean = name.replace('"', "");
    let parts: Vec<&str> = clean.split('.').collect();
    let (ns, table) = match parts.as_slice() {
        [t] => (default_ns.to_string(), (*t).to_string()),
        [ns, t] => ((*ns).to_string(), (*t).to_string()),
        // catalog.namespace.table — drop the catalog component.
        [_cat, ns, t] => ((*ns).to_string(), (*t).to_string()),
        _ => return None,
    };
    if table.is_empty() {
        return None;
    }
    Some(TableRef {
        namespace: ns,
        table,
    })
}

/// Shared authorizer handle used by the hook and the Flight SQL path.
pub type SharedAuthorizer = Arc<dyn Authorizer>;

/// Build the SQLSTATE 42501 (insufficient_privilege) error for a denied
/// statement, in the shape Postgres clients expect.
pub fn deny_error(principal: &str, action: Action, target: &TableRef) -> PgWireError {
    let verb = match action {
        Action::ReadData => "SELECT",
        Action::WriteData => "write (INSERT/UPDATE/DELETE)",
        Action::DropTable => "DROP",
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42501".to_string(),
        format!("permission denied: role \"{principal}\" cannot {verb} on {target}"),
    )))
}

/// Query hook that enforces authorization before any other hook or planning.
/// Registered first in the chain; on a denied statement it returns the 42501
/// error, which aborts the statement. Allowed statements fall through
/// (`None`) to normal processing.
pub struct AuthzHook {
    authorizer: SharedAuthorizer,
    default_namespace: String,
}

impl AuthzHook {
    pub fn new(authorizer: SharedAuthorizer, default_namespace: String) -> Self {
        AuthzHook {
            authorizer,
            default_namespace,
        }
    }

    /// Returns `Some(Err)` if the principal is denied, `None` if allowed.
    fn gate(
        &self,
        stmt: &Statement,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireError> {
        let principal = client
            .metadata()
            .get(METADATA_USER)
            .map(String::as_str)
            .unwrap_or("");
        match self
            .authorizer
            .authorize_sql(principal, stmt, &self.default_namespace)
        {
            Decision::Allow => None,
            Decision::Deny { action, target } => Some(deny_error(principal, action, &target)),
        }
    }
}

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::QueryHook;

#[async_trait]
impl QueryHook for AuthzHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _ctx: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        self.gate(statement, client).map(Err)
    }

    async fn handle_extended_parse_query(
        &self,
        sql: &Statement,
        _ctx: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        self.gate(sql, client).map(Err)
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _plan: &LogicalPlan,
        _params: &ParamValues,
        _ctx: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        self.gate(statement, client).map(Err)
    }
}

#[cfg(all(test, feature = "managed"))]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse1(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap()
    }

    fn authz(policy: &str) -> FileAuthorizer {
        FileAuthorizer::parse(policy).unwrap()
    }

    #[test]
    fn relation_implication() {
        assert!(Relation::Own.implies(Relation::Read));
        assert!(Relation::Own.implies(Relation::Write));
        assert!(Relation::Own.implies(Relation::Drop));
        assert!(Relation::Write.implies(Relation::Read));
        assert!(!Relation::Read.implies(Relation::Write));
        assert!(!Relation::Write.implies(Relation::Drop));
    }

    #[test]
    fn namespace_grant_inherits_to_tables() {
        let a = authz("grant analyst read demo\nmember alice analyst\n");
        let t = TableRef {
            namespace: "demo".into(),
            table: "trips".into(),
        };
        assert_eq!(a.check("alice", Action::ReadData, &t), Decision::Allow);
        // no write grant
        assert!(matches!(
            a.check("alice", Action::WriteData, &t),
            Decision::Deny { .. }
        ));
        // other namespace denied
        let other = TableRef {
            namespace: "secret".into(),
            table: "x".into(),
        };
        assert!(matches!(
            a.check("alice", Action::ReadData, &other),
            Decision::Deny { .. }
        ));
    }

    #[test]
    fn warehouse_owner_can_everything() {
        let a = authz("grant admin own *\n");
        let t = TableRef {
            namespace: "demo".into(),
            table: "trips".into(),
        };
        assert_eq!(a.check("admin", Action::ReadData, &t), Decision::Allow);
        assert_eq!(a.check("admin", Action::WriteData, &t), Decision::Allow);
        assert_eq!(a.check("admin", Action::DropTable, &t), Decision::Allow);
    }

    #[test]
    fn table_write_grant_is_scoped() {
        let a = authz("grant writer write demo.trips\n");
        let trips = TableRef {
            namespace: "demo".into(),
            table: "trips".into(),
        };
        let cities = TableRef {
            namespace: "demo".into(),
            table: "cities".into(),
        };
        assert_eq!(
            a.check("writer", Action::WriteData, &trips),
            Decision::Allow
        );
        assert_eq!(a.check("writer", Action::ReadData, &trips), Decision::Allow); // write⊇read
        assert!(matches!(
            a.check("writer", Action::WriteData, &cities),
            Decision::Deny { .. }
        ));
    }

    #[test]
    fn select_requires_read_on_all_joined_tables() {
        let a = authz("grant u read demo.trips\n"); // only trips, not cities
        let stmt = parse1("select * from demo.trips t join demo.cities c on t.city=c.city");
        assert!(matches!(
            a.authorize_sql("u", &stmt, "demo"),
            Decision::Deny { .. }
        ));
        let a2 = authz("grant u read demo\n");
        assert_eq!(a2.authorize_sql("u", &stmt, "demo"), Decision::Allow);
    }

    #[test]
    fn insert_requires_write() {
        let a = authz("grant r read demo\n");
        let stmt = parse1("insert into demo.trips values (1,'x',1.0,2.0)");
        assert!(matches!(
            a.authorize_sql("r", &stmt, "demo"),
            Decision::Deny { .. }
        ));
    }

    #[test]
    fn pg_catalog_reads_are_free() {
        let a = authz(""); // no grants at all
        let stmt = parse1("select current_database()");
        assert_eq!(a.authorize_sql("nobody", &stmt, "demo"), Decision::Allow);
        let stmt2 = parse1("select * from pg_catalog.pg_class");
        assert_eq!(a.authorize_sql("nobody", &stmt2, "demo"), Decision::Allow);
    }

    #[test]
    fn transitive_role_membership() {
        // alice -> senior -> analyst; analyst has read demo
        let a = authz("grant analyst read demo\nmember senior analyst\nmember alice senior\n");
        let t = TableRef {
            namespace: "demo".into(),
            table: "trips".into(),
        };
        assert_eq!(a.check("alice", Action::ReadData, &t), Decision::Allow);
    }

    #[test]
    fn default_namespace_resolves_unqualified() {
        let a = authz("grant u read demo.trips\n");
        let stmt = parse1("select * from trips");
        assert_eq!(a.authorize_sql("u", &stmt, "demo"), Decision::Allow);
    }
}
