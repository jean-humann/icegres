//! UPDATE/DELETE on the wire (SPEC B2/B3).
//!
//! datafusion-postgres plans SQL through DataFusion, whose Iceberg table
//! providers are append-only (`insert_into` only) — a raw `UPDATE`/`DELETE`
//! would fail in the planner. [`DmlHook`] intercepts exactly those two
//! statement kinds in datafusion-postgres's query-hook chain (simple AND
//! extended protocol), translates them into a [`DmlStatement`], and executes
//! them through the copy-on-write [`OverwriteEngine`](crate::overwrite),
//! answering with the standard `UPDATE n` / `DELETE n` command tags.
//!
//! Scope is deliberately strict — every unsupported form is REJECTED with a
//! clear error instead of being silently mis-executed:
//!
//! * single plain table target (no joins, `USING`, multi-table, `RETURNING`,
//!   `ORDER BY`/`LIMIT`, SQLite `OR` clauses);
//! * no subqueries in the predicate or assignment values (the engine
//!   evaluates expressions per data file, where a subquery over the same
//!   table would see only a slice of it);
//! * no placeholder parameters (`$1`) — bind values are not interpolated.
//!
//! Freshness: a committed DML moves the table's `main` ref; the snapshot-
//! aware read cache (cache.rs) detects the new metadata location on the next
//! scan, so DML follows the exact same read-your-writes path INSERT already
//! uses — no explicit invalidation required, on this server or any other.

use std::ops::ControlFlow;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DFSchema, ParamValues};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    self, Delete, Expr, FromTable, ObjectName, ObjectNamePart, Statement, TableFactor,
    TableWithJoins, Visit, Visitor,
};
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::QueryHook;

use crate::context::{CATALOG_NAME, DEFAULT_SCHEMA};
use crate::overwrite::{
    CommitConflict, ConstraintViolation, DmlKind, DmlStatement, OverwriteEngine,
};

/// Query hook translating UPDATE/DELETE into copy-on-write Iceberg commits.
pub struct DmlHook {
    engine: Arc<OverwriteEngine>,
}

impl DmlHook {
    pub fn new(engine: Arc<OverwriteEngine>) -> Self {
        Self { engine }
    }

    async fn run(&self, stmt: &Statement) -> PgWireResult<Response> {
        let (dml, tag_name) = match translate(stmt).map_err(reject)? {
            Some(parsed) => parsed,
            None => unreachable!("run() is only called for Update/Delete statements"),
        };
        let outcome = self
            .engine
            .execute(&dml)
            .await
            .map_err(|e| engine_error(&e))?;
        tracing::debug!(
            rows = outcome.rows,
            attempts = outcome.attempts,
            snapshot_id = outcome.snapshot_id,
            "DML hook completed"
        );
        Ok(Response::Execution(
            Tag::new(tag_name).with_rows(outcome.rows as usize),
        ))
    }
}

#[async_trait]
impl QueryHook for DmlHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        if !is_dml(statement) {
            return None;
        }
        Some(self.run(statement).await)
    }

    async fn handle_extended_parse_query(
        &self,
        sql: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if !is_dml(sql) {
            return None;
        }
        // Placeholder plan: execution happens in handle_extended_query; the
        // response carries a command tag, so an empty zero-row schema fits.
        Some(Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        if !is_dml(statement) {
            return None;
        }
        let has_params = match params {
            ParamValues::List(l) => !l.is_empty(),
            ParamValues::Map(m) => !m.is_empty(),
        };
        if has_params {
            return Some(Err(reject(anyhow::anyhow!(
                "parameterized UPDATE/DELETE ($n bind values) is not supported yet; \
                 inline the values"
            ))));
        }
        Some(self.run(statement).await)
    }
}

fn is_dml(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Update { .. } | Statement::Delete(_))
}

/// Parse `query`; if it is a single UPDATE/DELETE statement, return the
/// translated [`DmlStatement`] and its command tag. Used by `icegres sql`
/// so the CLI shares the server's DML path.
pub fn parse_single_dml(query: &str) -> anyhow::Result<Option<(DmlStatement, &'static str)>> {
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, query) else {
        // Not parseable by sqlparser: let DataFusion produce its own error.
        return Ok(None);
    };
    match statements.as_slice() {
        [stmt] if is_dml(stmt) => translate(stmt),
        _ => Ok(None),
    }
}

/// `feature_not_supported` (0A000): the statement form is out of scope.
fn reject(e: anyhow::Error) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        format!("{e:#}"),
    )))
}

/// Map an engine error to the wire: typed constraint violations
/// (23502/23505) and serialization failures (40001) keep their Postgres
/// sqlstate; everything else is `internal_error` (XX000).
pub(crate) fn engine_error(e: &anyhow::Error) -> PgWireError {
    let (code, msg) = if let Some(v) = e.downcast_ref::<ConstraintViolation>() {
        (v.sqlstate.to_string(), v.message.clone())
    } else if let Some(c) = e.downcast_ref::<CommitConflict>() {
        crate::metrics::metrics()
            .commit_conflicts_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ("40001".to_string(), c.message.clone())
    } else {
        ("XX000".to_string(), format!("{e:#}"))
    };
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".to_string(), code, msg)))
}

/// Translate a parsed statement into a [`DmlStatement`] (with its command
/// tag). `Ok(None)` when the statement is not UPDATE/DELETE. Also used by
/// the transaction hook (txn.rs) so buffered and autocommit DML share one
/// translation with identical scope checks.
pub(crate) fn translate(stmt: &Statement) -> anyhow::Result<Option<(DmlStatement, &'static str)>> {
    match stmt {
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
            limit,
        } => {
            anyhow::ensure!(from.is_none(), "UPDATE ... FROM is not supported");
            anyhow::ensure!(returning.is_none(), "UPDATE ... RETURNING is not supported");
            anyhow::ensure!(or.is_none(), "UPDATE OR ... is not supported");
            anyhow::ensure!(limit.is_none(), "UPDATE ... LIMIT is not supported");
            let (namespace, table_name, alias) = plain_table(table)?;
            let mut pairs = Vec::with_capacity(assignments.len());
            for a in assignments {
                let col = match &a.target {
                    ast::AssignmentTarget::ColumnName(name) => last_ident(name)?,
                    ast::AssignmentTarget::Tuple(_) => {
                        anyhow::bail!("tuple assignment targets are not supported")
                    }
                };
                anyhow::ensure!(
                    pairs.iter().all(|(c, _)| c != &col),
                    "column \"{col}\" is assigned more than once"
                );
                ensure_no_subquery(&a.value, "assignment value")?;
                pairs.push((col, a.value.to_string()));
            }
            anyhow::ensure!(!pairs.is_empty(), "UPDATE requires at least one assignment");
            if let Some(p) = selection {
                ensure_no_subquery(p, "WHERE clause")?;
            }
            Ok(Some((
                DmlStatement {
                    kind: DmlKind::Update { assignments: pairs },
                    namespace,
                    table: table_name,
                    alias,
                    predicate: selection.as_ref().map(|e| e.to_string()),
                },
                "UPDATE",
            )))
        }
        Statement::Delete(Delete {
            tables,
            from,
            using,
            selection,
            returning,
            order_by,
            limit,
        }) => {
            anyhow::ensure!(tables.is_empty(), "multi-table DELETE is not supported");
            anyhow::ensure!(using.is_none(), "DELETE ... USING is not supported");
            anyhow::ensure!(returning.is_none(), "DELETE ... RETURNING is not supported");
            anyhow::ensure!(order_by.is_empty(), "DELETE ... ORDER BY is not supported");
            anyhow::ensure!(limit.is_none(), "DELETE ... LIMIT is not supported");
            let twj = match from {
                FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => {
                    anyhow::ensure!(v.len() == 1, "DELETE requires exactly one target table");
                    &v[0]
                }
            };
            let (namespace, table_name, alias) = plain_table(twj)?;
            if let Some(p) = selection {
                ensure_no_subquery(p, "WHERE clause")?;
            }
            Ok(Some((
                DmlStatement {
                    kind: DmlKind::Delete,
                    namespace,
                    table: table_name,
                    alias,
                    predicate: selection.as_ref().map(|e| e.to_string()),
                },
                "DELETE",
            )))
        }
        _ => Ok(None),
    }
}

/// Extract `(namespace, table, alias)` from a join-free plain table factor.
fn plain_table(twj: &TableWithJoins) -> anyhow::Result<(String, String, Option<String>)> {
    anyhow::ensure!(
        twj.joins.is_empty(),
        "joined tables are not supported in UPDATE/DELETE"
    );
    let TableFactor::Table { name, alias, .. } = &twj.relation else {
        anyhow::bail!("only plain table targets are supported in UPDATE/DELETE");
    };
    let mut parts: Vec<String> = Vec::new();
    for part in &name.0 {
        let ObjectNamePart::Identifier(ident) = part else {
            anyhow::bail!("unsupported table name part in {name}");
        };
        parts.push(normalize_ident(ident));
    }
    let (namespace, table) = match parts.len() {
        1 => (DEFAULT_SCHEMA.to_string(), parts.pop().expect("len 1")),
        2 => {
            let t = parts.pop().expect("len 2");
            (parts.pop().expect("len 2"), t)
        }
        3 => {
            anyhow::ensure!(
                parts[0] == CATALOG_NAME,
                "unknown catalog {:?} (only {CATALOG_NAME:?} is served)",
                parts[0]
            );
            let t = parts.pop().expect("len 3");
            (parts.pop().expect("len 3"), t)
        }
        n => anyhow::bail!("table name with {n} parts is not supported"),
    };
    let alias = match alias {
        None => None,
        Some(a) => {
            anyhow::ensure!(
                a.columns.is_empty(),
                "column aliases on the target table are not supported"
            );
            Some(normalize_ident(&a.name))
        }
    };
    Ok((namespace, table, alias))
}

/// Last identifier of a (possibly qualified) column reference.
fn last_ident(name: &ObjectName) -> anyhow::Result<String> {
    match name.0.last() {
        Some(ObjectNamePart::Identifier(ident)) => Ok(normalize_ident(ident)),
        _ => anyhow::bail!("unsupported assignment target {name}"),
    }
}

/// Postgres identifier semantics: unquoted identifiers fold to lowercase,
/// quoted ones are taken verbatim (matches DataFusion's normalization of the
/// SQL text we pass through).
fn normalize_ident(ident: &ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Reject subqueries anywhere inside `expr` — the engine evaluates
/// expressions per data file, so a subquery would silently see partial data.
fn ensure_no_subquery(expr: &Expr, what: &str) -> anyhow::Result<()> {
    struct FindQuery;
    impl Visitor for FindQuery {
        type Break = ();
        fn pre_visit_query(&mut self, _query: &ast::Query) -> ControlFlow<()> {
            ControlFlow::Break(())
        }
    }
    if expr.visit(&mut FindQuery).is_break() {
        anyhow::bail!("subqueries in the {what} of UPDATE/DELETE are not supported");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0)
    }

    #[test]
    fn commit_conflict_maps_to_sqlstate_40001() {
        // The contract every conflict path relies on (single-table
        // commit_pinned AND the atomic multi-table commit_pinned_multi):
        // CommitConflict -> serialization_failure, retryable.
        let err = anyhow::anyhow!(crate::overwrite::CommitConflict {
            message: "could not serialize access due to concurrent update: test".to_string(),
        });
        match engine_error(&err) {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, "40001");
                assert!(info.message.contains("could not serialize access"));
            }
            other => panic!("expected UserError, got {other:?}"),
        }
    }

    #[test]
    fn translates_delete_with_predicate() {
        let (dml, tag) = translate(&parse("DELETE FROM demo.trips WHERE trip_id = 7"))
            .unwrap()
            .unwrap();
        assert_eq!(tag, "DELETE");
        assert!(matches!(dml.kind, DmlKind::Delete));
        assert_eq!(
            (dml.namespace.as_str(), dml.table.as_str()),
            ("demo", "trips")
        );
        assert_eq!(dml.predicate.as_deref(), Some("trip_id = 7"));
    }

    #[test]
    fn translates_update_with_default_namespace_and_alias() {
        let (dml, tag) = translate(&parse(
            "UPDATE trips t SET fare = fare * 2 WHERE t.city = 'X'",
        ))
        .unwrap()
        .unwrap();
        assert_eq!(tag, "UPDATE");
        assert_eq!(dml.namespace, DEFAULT_SCHEMA);
        assert_eq!(dml.alias.as_deref(), Some("t"));
        match &dml.kind {
            DmlKind::Update { assignments } => {
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].0, "fare");
                assert_eq!(assignments[0].1, "fare * 2");
            }
            other => panic!("expected update, got {other:?}"),
        }
    }

    #[test]
    fn folds_unquoted_idents_keeps_quoted() {
        let (dml, _) = translate(&parse("UPDATE Demo.TRIPS SET \"Fare\" = 1"))
            .unwrap()
            .unwrap();
        assert_eq!(dml.namespace, "demo");
        assert_eq!(dml.table, "trips");
        match &dml.kind {
            DmlKind::Update { assignments } => assert_eq!(assignments[0].0, "Fare"),
            other => panic!("expected update, got {other:?}"),
        }
    }

    #[test]
    fn rejects_subquery_predicate() {
        let err = translate(&parse(
            "DELETE FROM demo.trips WHERE trip_id IN (SELECT trip_id FROM demo.trips)",
        ))
        .unwrap_err();
        assert!(err.to_string().contains("subqueries"));
    }

    #[test]
    fn rejects_update_returning_and_delete_using() {
        assert!(translate(&parse("UPDATE t SET a = 1 RETURNING a")).is_err());
        assert!(translate(&parse("DELETE FROM t USING u WHERE t.a = u.a")).is_err());
    }

    #[test]
    fn non_dml_statements_pass_through() {
        assert!(translate(&parse("SELECT 1")).unwrap().is_none());
        assert!(translate(&parse("INSERT INTO t VALUES (1)"))
            .unwrap()
            .is_none());
    }
}
