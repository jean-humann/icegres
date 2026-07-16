//! `AS OF` time-travel sugar (roadmap-v2 P5):
//!
//! ```sql
//! SELECT ... FROM t AS OF TIMESTAMP '2026-07-01 12:00:00'   -- at/just-before
//! SELECT ... FROM t AS OF 4436304835314641572               -- exact snapshot
//! ```
//!
//! rewritten to the existing `table@snapshot_id` time-travel path (cache.rs):
//! the timestamp form resolves the table's snapshot-log entry AT or JUST
//! BEFORE the given instant (naive timestamps are read as UTC), the numeric
//! form pins that exact snapshot — then `t AS OF <version>` is spliced into
//! `"t@<snapshot_id>"` and the statement continues down the normal path
//! untouched. A timestamp BEFORE the table's first snapshot is a loud error
//! (there is nothing to read), as is a snapshot id (or a resolved snapshot)
//! that has been expired out of metadata.
//!
//! # Dialect note
//!
//! `AS OF` is DuckDB/Databricks-style sugar, NOT Postgres syntax — stock
//! Postgres rejects it, and so does the sqlparser grammar the wire stack
//! parses statements with. pgwire clients pass statements through as plain
//! SQL, so the rewrite runs on the RAW statement text BEFORE parsing: on the
//! simple protocol in [`crate::traced::TracedService::do_query`], on the
//! extended protocol in [`AsOfParser::parse_sql`] (both feed the rewritten
//! text to the stock parser), and in `icegres sql -e`. The Flight SQL
//! endpoint does not carry the sugar (use `"t@<snapshot_id>"` directly).
//!
//! # Gating (default behavior byte-identical)
//!
//! Statements without the exact pattern are NEVER touched: a cheap
//! allocation-free scan ([`might_contain_as_of`]) skips everything that
//! lacks an `AS OF` word pair, and the real matcher tokenizes with the same
//! sqlparser tokenizer the parser uses (so string literals, comments, and
//! quoted identifiers can never confuse it) and requires, exactly:
//! a table factor (`t` or `ns.t`, quoted or not) preceded by `FROM`/`JOIN`/
//! a comma, followed by `AS OF` and either `TIMESTAMP '<literal>'` or an
//! integer snapshot id. Anything else — e.g. `SELECT x AS of` (an alias
//! named "of") — falls through unchanged.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context as _, Result};
use arrow::array::{Array as _, ArrayRef, AsArray, StringArray};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use async_trait::async_trait;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer, Word};
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::FieldInfo;
use datafusion_postgres::pgwire::api::stmt::QueryParser;
use datafusion_postgres::pgwire::api::{ClientInfo, Type};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::DfSessionService;
use iceberg::{Catalog, TableIdent};

use crate::context::DEFAULT_SCHEMA;
use crate::overwrite::quote_ident;

/// The version selector after `AS OF`.
#[derive(Debug, Clone, PartialEq)]
enum AsOfSpec {
    /// `AS OF TIMESTAMP '<literal>'` — resolve at/just-before this instant.
    Timestamp(String),
    /// `AS OF <integer>` — this exact snapshot id.
    SnapshotId(i64),
}

/// One matched `<relation> AS OF <version>` occurrence in the raw SQL.
#[derive(Debug, Clone)]
struct Occurrence {
    /// Byte offset of the relation's first character.
    start: usize,
    /// Byte offset just past the version token (start of whatever follows).
    end: usize,
    /// Namespace, already case-normalized (`None` = the default schema at
    /// resolution time; the splice keeps it unqualified).
    namespace: Option<String>,
    /// Table name, already case-normalized.
    table: String,
    spec: AsOfSpec,
}

/// Allocation-free fast gate: does `sql` contain the word `AS` followed by
/// whitespace and the word `OF` (any case)? False positives just pay one
/// tokenize; false negatives are impossible for the exact gated syntax
/// (which always separates the two words with whitespace).
pub fn might_contain_as_of(sql: &str) -> bool {
    let b = sql.as_bytes();
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$';
    let mut i = 0;
    while i + 1 < b.len() {
        if b[i].eq_ignore_ascii_case(&b'a')
            && b[i + 1].eq_ignore_ascii_case(&b's')
            && (i == 0 || !is_word(b[i - 1]))
        {
            let mut j = i + 2;
            if j < b.len() && !is_word(b[j]) {
                while j < b.len() && b[j].is_ascii_whitespace() {
                    j += 1;
                }
                if j + 1 < b.len()
                    && b[j].eq_ignore_ascii_case(&b'o')
                    && b[j + 1].eq_ignore_ascii_case(&b'f')
                    && (j + 2 >= b.len() || !is_word(b[j + 2]))
                {
                    return true;
                }
            }
        }
        i += 1;
    }
    false
}

/// Case-normalize an identifier word the way Postgres resolution does:
/// unquoted identifiers fold to lowercase, quoted ones are taken verbatim.
fn norm_ident(w: &Word) -> String {
    if w.quote_style.is_some() {
        w.value.clone()
    } else {
        w.value.to_lowercase()
    }
}

/// Map a 1-based sqlparser `(line, column)` location to a byte offset in
/// `sql` (columns count characters).
fn byte_offset(sql: &str, line: u64, column: u64) -> Option<usize> {
    let mut cur_line = 1u64;
    let mut cur_col = 1u64;
    for (idx, ch) in sql.char_indices() {
        if cur_line == line && cur_col == column {
            return Some(idx);
        }
        if ch == '\n' {
            cur_line += 1;
            cur_col = 1;
        } else {
            cur_col += 1;
        }
    }
    (cur_line == line && cur_col == column).then_some(sql.len())
}

/// Find every gated `<relation> AS OF <version>` occurrence in `sql`. An
/// `AS OF` pair that is not the exact sugar (wrong context, unsupported
/// version form) is left alone — this function only errors on occurrences
/// that ARE the sugar but cannot be rewritten (3-part relation names).
fn find_occurrences(sql: &str) -> Result<Vec<Occurrence>> {
    let dialect = PostgreSqlDialect {};
    let tokens: Vec<TokenWithSpan> = match Tokenizer::new(&dialect, sql).tokenize_with_location() {
        Ok(t) => t,
        // Untokenizable text is not our business: let the normal parser
        // produce its own (unchanged) error downstream.
        Err(_) => return Ok(Vec::new()),
    };
    // Indices of non-whitespace tokens, in order.
    let sig: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t.token, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();
    let word_kw = |pos: usize, kw: Keyword| -> bool {
        matches!(&tokens[sig[pos]].token,
                 Token::Word(w) if w.keyword == kw && w.quote_style.is_none())
    };
    let mut out = Vec::new();
    for k in 0..sig.len() {
        if !word_kw(k, Keyword::AS) || k + 1 >= sig.len() || !word_kw(k + 1, Keyword::OF) {
            continue;
        }
        // --- version selector after AS OF -------------------------------
        let (spec, last_sig) = if k + 3 < sig.len() && word_kw(k + 2, Keyword::TIMESTAMP) {
            match &tokens[sig[k + 3]].token {
                Token::SingleQuotedString(s) => (AsOfSpec::Timestamp(s.clone()), k + 3),
                _ => continue,
            }
        } else if k + 2 < sig.len() {
            match &tokens[sig[k + 2]].token {
                Token::Number(n, _) => match n.parse::<i64>() {
                    Ok(id) => (AsOfSpec::SnapshotId(id), k + 2),
                    Err(_) => continue,
                },
                _ => continue,
            }
        } else {
            continue;
        };
        // --- the relation before AS: `t` or `ns.t` ----------------------
        let ident_at = |pos: usize| -> Option<&Word> {
            match &tokens[sig[pos]].token {
                Token::Word(w) if w.quote_style.is_some() || w.keyword == Keyword::NoKeyword => {
                    Some(w)
                }
                _ => None,
            }
        };
        if k == 0 {
            continue;
        }
        let Some(table_word) = ident_at(k - 1) else {
            continue;
        };
        let mut chain_start = k - 1;
        let mut namespace: Option<String> = None;
        if k >= 3 && matches!(tokens[sig[k - 2]].token, Token::Period) {
            if let Some(ns_word) = ident_at(k - 3) {
                // A third name part (`cat.ns.t AS OF ...`) is the sugar but
                // unsupported: error loudly instead of a confusing pass-through.
                if k >= 5 && matches!(tokens[sig[k - 4]].token, Token::Period) {
                    bail!(
                        "AS OF supports <table> or <namespace>.<table> relations only \
                         (got a 3-part name before AS OF)"
                    );
                }
                namespace = Some(norm_ident(ns_word));
                chain_start = k - 3;
            }
        }
        // --- context: the chain must be a table factor ------------------
        if chain_start == 0 {
            continue;
        }
        let before = &tokens[sig[chain_start - 1]].token;
        let is_table_context = matches!(before, Token::Comma)
            || matches!(before, Token::Word(w)
                        if w.quote_style.is_none()
                            && matches!(w.keyword, Keyword::FROM | Keyword::JOIN));
        if !is_table_context {
            continue;
        }
        // --- byte range: relation start .. start of whatever follows ----
        let start_loc = tokens[sig[chain_start]].span.start;
        let start = byte_offset(sql, start_loc.line, start_loc.column)
            .ok_or_else(|| anyhow!("AS OF rewrite: token location out of range"))?;
        let end = match tokens.get(sig[last_sig] + 1) {
            Some(next) => byte_offset(sql, next.span.start.line, next.span.start.column)
                .ok_or_else(|| anyhow!("AS OF rewrite: token location out of range"))?,
            None => sql.len(),
        };
        out.push(Occurrence {
            start,
            end,
            namespace,
            table: norm_ident(table_word),
            spec,
        });
    }
    Ok(out)
}

/// Parse a SQL timestamp literal to epoch milliseconds via Arrow's caster
/// (the same coercion the engine applies to `TIMESTAMP '...'` literals).
/// Naive literals are read as UTC; explicit offsets are honored.
fn parse_timestamp_ms(literal: &str) -> Result<i64> {
    let arr: ArrayRef = Arc::new(StringArray::from(vec![literal]));
    let casted = cast_with_options(
        &arr,
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        &CastOptions::default(), // safe: unparseable -> null, checked below
    )
    .with_context(|| format!("cannot parse AS OF TIMESTAMP {literal:?}"))?;
    let ts = casted.as_primitive::<TimestampMicrosecondType>();
    if ts.is_null(0) {
        bail!(
            "cannot parse AS OF TIMESTAMP {literal:?}: use an ISO-8601 timestamp like \
             '2026-07-01 12:00:00' (naive timestamps are read as UTC)"
        );
    }
    Ok(ts.value(0) / 1000)
}

/// Resolve the snapshot-log entry at or just before `ts_ms`. `log` is the
/// table's snapshot log as `(timestamp_ms, snapshot_id)` in chronological
/// order; `known` answers whether a snapshot id is still in metadata.
/// Pure — unit-tested on the boundaries (exact hit, between entries,
/// before the first entry, expired resolution).
fn snapshot_at(
    log: &[(i64, i64)],
    known: &impl Fn(i64) -> bool,
    ts_ms: i64,
    what: &str,
) -> Result<i64> {
    let Some((entry_ts, id)) = log.iter().rev().find(|(t, _)| *t <= ts_ms) else {
        let earliest = log.first();
        bail!(
            "no snapshot of {what} exists at or before the requested time: the table's \
             first snapshot is {} — AS OF cannot read before a table's history begins",
            earliest
                .map(|(t, id)| format!("{id} at epoch-ms {t}"))
                .unwrap_or_else(|| "absent (empty snapshot log)".into())
        );
    };
    if !known(*id) {
        bail!(
            "the snapshot of {what} at the requested time (snapshot {id}, committed at \
             epoch-ms {entry_ts}) has been expired out of table metadata; expired \
             history is not readable"
        );
    }
    Ok(*id)
}

/// Rewrite every gated `AS OF` occurrence in `sql` to the
/// `"table@snapshot_id"` form, resolving snapshots against `catalog`.
/// `Ok(None)` = no occurrence, the statement is untouched (the common path).
pub async fn rewrite_as_of(catalog: &dyn Catalog, sql: &str) -> Result<Option<String>> {
    if !might_contain_as_of(sql) {
        return Ok(None);
    }
    let mut occurrences = find_occurrences(sql)?;
    if occurrences.is_empty() {
        return Ok(None);
    }
    // Splice back-to-front so earlier byte offsets stay valid.
    occurrences.sort_by_key(|o| o.start);
    let mut rewritten = sql.to_string();
    for occ in occurrences.iter().rev() {
        let ns = occ.namespace.as_deref().unwrap_or(DEFAULT_SCHEMA);
        let ident = TableIdent::from_strs([ns, occ.table.as_str()])
            .map_err(|e| anyhow!("bad table identifier in AS OF relation: {e}"))?;
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(|e| anyhow!("AS OF: failed to load table {ident}: {e}"))?;
        let metadata = table.metadata();
        let snapshot_id = match &occ.spec {
            AsOfSpec::SnapshotId(id) => {
                if metadata.snapshot_by_id(*id).is_none() {
                    bail!(
                        "AS OF {id}: snapshot {id} does not exist in table {ident} \
                         (expired, or never committed)"
                    );
                }
                *id
            }
            AsOfSpec::Timestamp(lit) => {
                let ts_ms = parse_timestamp_ms(lit)?;
                let log: Vec<(i64, i64)> = metadata
                    .history()
                    .iter()
                    .map(|l| (l.timestamp_ms, l.snapshot_id))
                    .collect();
                snapshot_at(
                    &log,
                    &|id| metadata.snapshot_by_id(id).is_some(),
                    ts_ms,
                    &ident.to_string(),
                )?
            }
        };
        let replacement = match &occ.namespace {
            Some(ns) => format!(
                "{}.{}",
                quote_ident(ns),
                quote_ident(&format!("{}@{snapshot_id}", occ.table))
            ),
            None => quote_ident(&format!("{}@{snapshot_id}", occ.table)),
        };
        // Trailing space: the spliced range may have swallowed the
        // whitespace token after the version literal.
        rewritten.replace_range(occ.start..occ.end, &format!("{replacement} "));
    }
    tracing::debug!(rewritten = %rewritten, "AS OF: rewrote to the table@snapshot path");
    Ok(Some(rewritten))
}

/// Shared rewriter handle for the wire layers.
pub struct AsOfRewriter {
    catalog: Arc<dyn Catalog>,
}

impl AsOfRewriter {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Rewrite for a pgwire code path (errors map to statement errors).
    pub async fn rewrite(&self, sql: &str) -> PgWireResult<Option<String>> {
        rewrite_as_of(self.catalog.as_ref(), sql)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))
    }
}

/// The extended-protocol query parser with the `AS OF` pre-rewrite: rewrites
/// the raw SQL (when the exact sugar is present), then delegates to the
/// stock datafusion-postgres parser. Statements without the sugar take the
/// stock path byte-identically.
pub struct AsOfParser {
    inner: Arc<<DfSessionService as datafusion_postgres::pgwire::api::query::ExtendedQueryHandler>::QueryParser>,
    rewriter: Arc<AsOfRewriter>,
}

impl AsOfParser {
    pub fn new(
        inner: Arc<
            <DfSessionService as datafusion_postgres::pgwire::api::query::ExtendedQueryHandler>::QueryParser,
        >,
        rewriter: Arc<AsOfRewriter>,
    ) -> Self {
        Self { inner, rewriter }
    }
}

#[async_trait]
impl QueryParser for AsOfParser {
    type Statement =
        <<DfSessionService as datafusion_postgres::pgwire::api::query::ExtendedQueryHandler>::QueryParser as QueryParser>::Statement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        match self.rewriter.rewrite(sql).await? {
            Some(rewritten) => self.inner.parse_sql(client, &rewritten, types).await,
            None => self.inner.parse_sql(client, sql, types).await,
        }
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        self.inner.get_parameter_types(stmt)
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        self.inner.get_result_schema(stmt, column_format)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn occ(sql: &str) -> Vec<Occurrence> {
        find_occurrences(sql).unwrap()
    }

    #[test]
    fn fast_gate_matches_only_word_pairs() {
        assert!(might_contain_as_of("select * from t AS OF 5"));
        assert!(might_contain_as_of("select * from t as   of 5"));
        assert!(might_contain_as_of("select * from t\nAS\tOF 5"));
        assert!(!might_contain_as_of("select * from trips"));
        assert!(!might_contain_as_of("select alias, office from t"));
        assert!(!might_contain_as_of("select a as offset from t"));
    }

    #[test]
    fn matches_snapshot_id_form() {
        let o = occ("SELECT * FROM trips AS OF 42");
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].table, "trips");
        assert_eq!(o[0].namespace, None);
        assert_eq!(o[0].spec, AsOfSpec::SnapshotId(42));
        let sql = "SELECT * FROM demo.trips AS OF 42 WHERE x = 'AS OF 7'";
        let o = occ(sql);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].namespace.as_deref(), Some("demo"));
        // The byte range covers exactly `demo.trips AS OF 42` (the
        // following whitespace token stays outside the splice).
        assert_eq!(&sql[o[0].start..o[0].end], "demo.trips AS OF 42");
    }

    #[test]
    fn matches_timestamp_form_and_quoted_idents() {
        let o = occ("select a from \"Demo\".\"Trips\" as of timestamp '2026-07-01 00:00:00', u");
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].namespace.as_deref(), Some("Demo"));
        assert_eq!(o[0].table, "Trips");
        assert_eq!(o[0].spec, AsOfSpec::Timestamp("2026-07-01 00:00:00".into()));
        // Unquoted identifiers fold to lowercase (Postgres semantics).
        let o = occ("select a from Trips as of 7");
        assert_eq!(o[0].table, "trips");
    }

    #[test]
    fn matches_join_and_comma_factors_and_multiple_occurrences() {
        let sql = "select * from a AS OF 1 join b AS OF 2 on a.x = b.x";
        let o = occ(sql);
        assert_eq!(o.len(), 2);
        assert_eq!(o[0].table, "a");
        assert_eq!(o[1].table, "b");
        let o = occ("select * from a, b as of 3");
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].table, "b");
    }

    #[test]
    fn gates_out_non_sugar() {
        // Alias named "of" (no version selector follows).
        assert!(occ("select a AS of FROM t").is_empty());
        // AS OF inside a string literal.
        assert!(occ("select * from t where note = 'x AS OF 5'").is_empty());
        // Not a table-factor context.
        assert!(occ("select f(t AS OF 3)").is_empty());
        // Unsupported version selector shapes fall through unchanged.
        assert!(occ("select * from t AS OF 'raw-string'").is_empty());
        assert!(occ("select * from t AS OF now()").is_empty());
        // 3-part relation names are the sugar but unsupported: loud error.
        assert!(find_occurrences("select * from c.n.t AS OF 5").is_err());
    }

    #[test]
    fn timestamp_literals_parse_as_utc() {
        assert_eq!(parse_timestamp_ms("1970-01-01 00:00:00").unwrap(), 0);
        assert_eq!(parse_timestamp_ms("1970-01-01 00:00:01").unwrap(), 1000);
        // Explicit offsets are honored.
        assert_eq!(parse_timestamp_ms("1970-01-01 01:00:00+01:00").unwrap(), 0);
        assert!(parse_timestamp_ms("not a timestamp").is_err());
    }

    #[test]
    fn snapshot_at_boundaries() {
        let log = [(1000, 10), (2000, 20), (3000, 30)];
        let all_known = |_: i64| true;
        // Exact hit.
        assert_eq!(snapshot_at(&log, &all_known, 2000, "t").unwrap(), 20);
        // Between two entries -> the one just before.
        assert_eq!(snapshot_at(&log, &all_known, 2999, "t").unwrap(), 20);
        // After the last -> the head.
        assert_eq!(snapshot_at(&log, &all_known, 9999, "t").unwrap(), 30);
        // Before the first -> loud error.
        let err = snapshot_at(&log, &all_known, 999, "t").unwrap_err();
        assert!(format!("{err:#}").contains("before a table's history begins"));
        // Empty log -> loud error.
        assert!(snapshot_at(&[], &all_known, 1000, "t").is_err());
        // Resolved snapshot expired out of metadata -> loud error.
        let known = |id: i64| id != 20;
        let err = snapshot_at(&log, &known, 2500, "t").unwrap_err();
        assert!(format!("{err:#}").contains("expired"));
    }

    #[test]
    fn byte_offsets_handle_multibyte_and_newlines() {
        let sql = "select 'é'\nfrom t AS OF 5";
        let o = occ(sql);
        assert_eq!(o.len(), 1);
        assert_eq!(&sql[o[0].start..o[0].end], "t AS OF 5");
    }
}
