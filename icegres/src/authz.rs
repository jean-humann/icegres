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
//! [`AuthzHook`] runs first in the query-hook chain on the pgwire path; the
//! Flight SQL path enforces the same policy per RPC in `flight.rs`
//! (`check_sql` / `check_write`, resolving the bearer token to the
//! authenticated principal before calling [`Authorizer::authorize_sql`]).
//! Each statement is mapped to
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
use datafusion::sql::sqlparser::ast::{
    CopySource, ObjectName, SetExpr, Statement, TableObject, Visit, Visitor,
};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    /// The statement contains a relation-bearing form that the SQL-to-policy
    /// mapper does not understand. This is deliberately a denial: treating a
    /// future parser variant as requiring no checks would reopen the exact
    /// fail-open class this layer exists to prevent.
    DenyUnsupported,
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
        let Ok(checks) = required_checks(stmt, default_namespace) else {
            return Decision::DenyUnsupported;
        };
        for (action, target) in checks {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorizationMappingError;

impl fmt::Display for AuthorizationMappingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unsupported relation-bearing SQL statement")
    }
}

/// Map a statement to the (action, table) checks it requires. The mapper is
/// recursive and fail-closed: a relation-bearing form that is not explicitly
/// classified returns an error rather than silently authorizing the query.
pub fn required_checks(
    stmt: &Statement,
    default_ns: &str,
) -> std::result::Result<Vec<(Action, TableRef)>, AuthorizationMappingError> {
    let mut checks = Vec::new();
    collect_statement_checks(stmt, default_ns, &mut checks)?;
    // Metadata-schema reads are deliberately outside the data-plane policy;
    // writes and drops are never exempted merely because their target name is
    // pg_catalog-shaped.
    checks.retain(|(action, t)| *action != Action::ReadData || !is_system_schema(&t.namespace));
    // A visitor can encounter the same relation in nested shapes and a write
    // target is intentionally also collected as a read. Keep policy calls
    // deterministic and minimal without weakening either requirement.
    let mut seen = HashSet::new();
    checks.retain(|check| seen.insert(check.clone()));
    Ok(checks)
}

fn collect_statement_checks(
    stmt: &Statement,
    default_ns: &str,
    checks: &mut Vec<(Action, TableRef)>,
) -> std::result::Result<(), AuthorizationMappingError> {
    match stmt {
        Statement::Query(q) => collect_query_checks(q, default_ns, checks)?,
        Statement::Insert(insert) => {
            if let TableObject::TableName(name) = &insert.table {
                checks.push((Action::WriteData, object_name_to_ref(name, default_ns)?));
            } else {
                return Err(AuthorizationMappingError);
            }
            collect_relation_reads(stmt, default_ns, checks)?;
        }
        Statement::Update { table, .. } => {
            checks.push((
                Action::WriteData,
                table_factor_to_ref(&table.relation, default_ns)?,
            ));
            collect_relation_reads(stmt, default_ns, checks)?;
        }
        Statement::Delete(del) => {
            for name in &del.tables {
                checks.push((Action::WriteData, object_name_to_ref(name, default_ns)?));
            }
            let froms = match &del.from {
                sqlparser::ast::FromTable::WithFromKeyword(f)
                | sqlparser::ast::FromTable::WithoutKeyword(f) => f,
            };
            for twj in froms {
                checks.push((
                    Action::WriteData,
                    table_factor_to_ref(&twj.relation, default_ns)?,
                ));
            }
            collect_relation_reads(stmt, default_ns, checks)?;
        }
        Statement::Merge { table, .. } => {
            checks.push((Action::WriteData, table_factor_to_ref(table, default_ns)?));
            collect_relation_reads(stmt, default_ns, checks)?;
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
                    checks.push((action, object_name_to_ref(table_name, default_ns)?));
                }
                CopySource::Query(q) => collect_query_checks(q, default_ns, checks)?,
            }
            let _ = target; // STDIN/STDOUT/file target does not add a table
        }
        Statement::Drop { names, .. } => {
            for name in names {
                checks.push((Action::DropTable, object_name_to_ref(name, default_ns)?));
            }
        }
        // EXPLAIN still plans and exposes the referenced relations; ANALYZE
        // additionally executes them. Both therefore require the inner
        // statement's exact permissions.
        Statement::Explain { statement, .. } => {
            collect_statement_checks(statement, default_ns, checks)?;
        }
        Statement::CreateView { query, .. } => {
            collect_query_checks(query, default_ns, checks)?;
        }
        Statement::CreateTable(create) => {
            if let Some(query) = &create.query {
                collect_query_checks(query, default_ns, checks)?;
            }
            if let Some(clone) = &create.clone {
                checks.push((Action::ReadData, object_name_to_ref(clone, default_ns)?));
            }
            if let Some(like) = &create.like {
                let name = match like {
                    sqlparser::ast::CreateTableLikeKind::Parenthesized(like)
                    | sqlparser::ast::CreateTableLikeKind::Plain(like) => &like.name,
                };
                checks.push((Action::ReadData, object_name_to_ref(name, default_ns)?));
            }
        }
        // These are explicitly metadata/session operations. Keeping the list
        // positive makes a future relation-bearing parser variant hit the
        // fail-closed fallback below.
        Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowViews { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowCollation { .. }
        | Statement::ExplainTable { .. }
        | Statement::Set { .. }
        | Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. } => {}
        _ => {
            let mut relations = Vec::new();
            collect_relation_reads(stmt, default_ns, &mut relations)?;
            if !relations.is_empty() {
                return Err(AuthorizationMappingError);
            }
        }
    }
    Ok(())
}

/// Whether `stmt` is side-effect-free and therefore admissible on a
/// `--read-only` Flight listener.
///
/// This is a distinct concern from [`required_checks`], which enumerates the
/// ReBAC data-plane checks a statement needs: DDL such as `CREATE TABLE` or
/// `ALTER` requires no table-grant check (and so yields none there), yet must
/// still be refused on a read-only endpoint. This predicate is therefore
/// **fail-closed** — a statement form not positively known to be read-only
/// (any DML or DDL: INSERT/UPDATE/DELETE/MERGE, CREATE/CTAS, ALTER, DROP,
/// TRUNCATE, COPY … FROM, or a form added by a future parser) is treated as a
/// write. Classification is by statement form, never string matching, so
/// comments or whitespace cannot disguise a write.
pub fn is_read_only(stmt: &Statement) -> bool {
    match stmt {
        // A top-level query is read-only only if its body and every CTE are
        // reads: sqlparser wraps a data-modifying statement in `Statement::Query`
        // when it is written as `WITH t AS (…) INSERT … SELECT …`, and a CTE can
        // itself be a `DELETE … RETURNING`. Inspect the tree rather than trust
        // the outer form, so the guard does not lean on the engine rejecting
        // these at planning.
        Statement::Query(q) => query_is_read_only(q),
        // Read-only session and metadata forms.
        Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowViews { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowCollation { .. }
        | Statement::ExplainTable { .. }
        | Statement::Set { .. }
        | Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. } => true,
        // EXPLAIN is a read UNLESS ANALYZE actually executes the inner
        // statement — then it is read-only only if that statement is.
        Statement::Explain {
            analyze, statement, ..
        } => !*analyze || is_read_only(statement),
        // DML, DDL, COPY, and any unrecognized form: refuse.
        _ => false,
    }
}

/// Whether a (possibly CTE-bearing) query only reads — no data-modifying CTE
/// and no write in the body's set-expression tree.
fn query_is_read_only(query: &sqlparser::ast::Query) -> bool {
    if let Some(with) = &query.with {
        if with
            .cte_tables
            .iter()
            .any(|cte| !query_is_read_only(&cte.query))
        {
            return false;
        }
    }
    set_expr_is_read_only(&query.body)
}

/// Whether a query body's set-expression is a pure read. `Insert`/`Update`/
/// `Delete`/`Merge` embedded in a query body are writes; fail closed on any
/// unrecognized future variant.
fn set_expr_is_read_only(body: &sqlparser::ast::SetExpr) -> bool {
    use sqlparser::ast::SetExpr;
    match body {
        SetExpr::Select(_) | SetExpr::Values(_) | SetExpr::Table(_) => true,
        SetExpr::Query(q) => query_is_read_only(q),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_is_read_only(left) && set_expr_is_read_only(right)
        }
        _ => false,
    }
}

fn collect_query_checks(
    query: &sqlparser::ast::Query,
    default_ns: &str,
    out: &mut Vec<(Action, TableRef)>,
) -> std::result::Result<(), AuthorizationMappingError> {
    collect_relation_reads(query, default_ns, out)?;
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_writes(&cte.query, default_ns, out)?;
        }
    }
    collect_set_expr_writes(&query.body, default_ns, out)
}

fn collect_query_writes(
    query: &sqlparser::ast::Query,
    default_ns: &str,
    out: &mut Vec<(Action, TableRef)>,
) -> std::result::Result<(), AuthorizationMappingError> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_writes(&cte.query, default_ns, out)?;
        }
    }
    collect_set_expr_writes(&query.body, default_ns, out)
}

fn collect_set_expr_writes(
    body: &SetExpr,
    default_ns: &str,
    out: &mut Vec<(Action, TableRef)>,
) -> std::result::Result<(), AuthorizationMappingError> {
    match body {
        SetExpr::Query(q) => collect_query_writes(q, default_ns, out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_writes(left, default_ns, out)?;
            collect_set_expr_writes(right, default_ns, out)
        }
        SetExpr::Insert(stmt)
        | SetExpr::Update(stmt)
        | SetExpr::Delete(stmt)
        | SetExpr::Merge(stmt) => collect_statement_checks(stmt, default_ns, out),
        SetExpr::Select(_) | SetExpr::Values(_) | SetExpr::Table(_) => Ok(()),
    }
}

fn collect_relation_reads<V: Visit>(
    value: &V,
    default_ns: &str,
    out: &mut Vec<(Action, TableRef)>,
) -> std::result::Result<(), AuthorizationMappingError> {
    struct RelationReadVisitor<'a> {
        default_ns: &'a str,
        out: &'a mut Vec<(Action, TableRef)>,
        cte_scopes: Vec<CteScope>,
    }

    struct CteScope {
        /// CTE aliases visible at the current traversal position. For a
        /// non-recursive WITH this grows after each definition; WITH
        /// RECURSIVE makes all aliases visible inside every definition.
        visible: HashSet<String>,
        /// Child-query address -> alias to expose once that CTE definition
        /// has been traversed. Addresses are only compared during this
        /// synchronous AST walk; they are never dereferenced or retained.
        definitions: Vec<(usize, String)>,
        activate_in_parent: Option<String>,
    }

    impl Visitor for RelationReadVisitor<'_> {
        type Break = AuthorizationMappingError;

        fn pre_visit_query(
            &mut self,
            query: &sqlparser::ast::Query,
        ) -> std::ops::ControlFlow<Self::Break> {
            let address = query as *const sqlparser::ast::Query as usize;
            let activate_in_parent = self.cte_scopes.last().and_then(|scope| {
                scope
                    .definitions
                    .iter()
                    .find(|(child, _)| *child == address)
                    .map(|(_, name)| name.clone())
            });
            let (visible, definitions) = query
                .with
                .as_ref()
                .map(|with| {
                    let definitions: Vec<_> = with
                        .cte_tables
                        .iter()
                        .map(|cte| {
                            (
                                cte.query.as_ref() as *const sqlparser::ast::Query as usize,
                                normalize_ident(&cte.alias.name),
                            )
                        })
                        .collect();
                    let visible = if with.recursive {
                        definitions.iter().map(|(_, name)| name.clone()).collect()
                    } else {
                        HashSet::new()
                    };
                    (visible, definitions)
                })
                .unwrap_or_default();
            self.cte_scopes.push(CteScope {
                visible,
                definitions,
                activate_in_parent,
            });
            std::ops::ControlFlow::Continue(())
        }

        fn post_visit_query(
            &mut self,
            _query: &sqlparser::ast::Query,
        ) -> std::ops::ControlFlow<Self::Break> {
            let completed = self.cte_scopes.pop().expect("query scope pushed");
            if let (Some(parent), Some(alias)) =
                (self.cte_scopes.last_mut(), completed.activate_in_parent)
            {
                parent.visible.insert(alias);
            }
            std::ops::ControlFlow::Continue(())
        }

        fn pre_visit_relation(&mut self, name: &ObjectName) -> std::ops::ControlFlow<Self::Break> {
            let parts = match normalized_object_parts(name) {
                Ok(parts) => parts,
                Err(err) => return std::ops::ControlFlow::Break(err),
            };
            if let [name] = parts.as_slice() {
                if self
                    .cte_scopes
                    .iter()
                    .rev()
                    .any(|scope| scope.visible.contains(name))
                {
                    return std::ops::ControlFlow::Continue(());
                }
            }
            let target = match object_name_to_ref(name, self.default_ns) {
                Ok(target) => target,
                Err(err) => return std::ops::ControlFlow::Break(err),
            };
            self.out.push((Action::ReadData, target));
            std::ops::ControlFlow::Continue(())
        }
    }

    let mut visitor = RelationReadVisitor {
        default_ns,
        out,
        cte_scopes: Vec::new(),
    };
    match value.visit(&mut visitor) {
        std::ops::ControlFlow::Continue(()) => Ok(()),
        std::ops::ControlFlow::Break(err) => Err(err),
    }
}

fn table_factor_to_ref(
    factor: &sqlparser::ast::TableFactor,
    default_ns: &str,
) -> std::result::Result<TableRef, AuthorizationMappingError> {
    if let sqlparser::ast::TableFactor::Table { name, .. } = factor {
        object_name_to_ref(name, default_ns)
    } else {
        Err(AuthorizationMappingError)
    }
}

/// Resolve a possibly-qualified object name (`trips`, `demo.trips`,
/// `icegres.demo.trips`) to a `namespace.table`, defaulting the namespace.
/// Drops the optional catalog component while preserving quoted identifier
/// boundaries and matching DataFusion's lowercase normalization for unquoted
/// identifiers.
fn object_name_to_ref(
    name: &ObjectName,
    default_ns: &str,
) -> std::result::Result<TableRef, AuthorizationMappingError> {
    let parts = normalized_object_parts(name)?;
    let (ns, table) = match parts.as_slice() {
        [t] => (default_ns.to_string(), t.clone()),
        [ns, t] => (ns.clone(), t.clone()),
        // catalog.namespace.table — drop the catalog component.
        [_cat, ns, t] => (ns.clone(), t.clone()),
        _ => return Err(AuthorizationMappingError),
    };
    if table.is_empty() {
        return Err(AuthorizationMappingError);
    }
    Ok(TableRef {
        namespace: ns,
        table,
    })
}

fn normalized_object_parts(
    name: &ObjectName,
) -> std::result::Result<Vec<String>, AuthorizationMappingError> {
    name.0
        .iter()
        .map(|part| {
            let ident = part.as_ident().ok_or(AuthorizationMappingError)?;
            Ok(normalize_ident(ident))
        })
        .collect()
}

fn normalize_ident(ident: &sqlparser::ast::Ident) -> String {
    match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_ascii_lowercase(),
    }
}

/// Shared authorizer handle used by the hook and the Flight SQL path.
pub type SharedAuthorizer = Arc<dyn Authorizer>;

/// Human-readable permission-denied message, shared by the pgwire error
/// and the Flight SQL `Status::permission_denied` so both wire protocols
/// report a denial identically.
pub fn deny_message(principal: &str, action: Action, target: &TableRef) -> String {
    let verb = match action {
        Action::ReadData => "SELECT",
        Action::WriteData => "write (INSERT/UPDATE/DELETE)",
        Action::DropTable => "DROP",
    };
    format!("permission denied: role \"{principal}\" cannot {verb} on {target}")
}

/// Return the wire-safe message for a denial, or `None` for an allowed
/// decision. Shared by pgwire and Flight so the fail-closed mapping path is
/// enforced identically on both protocols.
pub fn decision_denial_message(principal: &str, decision: &Decision) -> Option<String> {
    match decision {
        Decision::Allow => None,
        Decision::Deny { action, target } => Some(deny_message(principal, *action, target)),
        Decision::DenyUnsupported => Some(format!(
            "permission denied: role \"{principal}\" cannot execute an unsupported \
             relation-bearing SQL statement"
        )),
    }
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
        let decision = self
            .authorizer
            .authorize_sql(principal, stmt, &self.default_namespace);
        decision_denial_message(principal, &decision).map(|message| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42501".to_string(),
                message,
            )))
        })
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

#[cfg(test)]
#[cfg(feature = "managed")]
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

    #[test]
    fn explain_analyze_recurses_into_writes() {
        let a = authz("grant u read demo\n");
        let stmt = parse1("explain analyze insert into demo.trips values (1)");
        assert!(matches!(
            a.authorize_sql("u", &stmt, "demo"),
            Decision::Deny {
                action: Action::WriteData,
                ..
            }
        ));
    }

    #[test]
    fn explain_without_analyze_still_requires_source_read() {
        let a = authz("");
        let stmt = parse1("explain select * from secret.data");
        assert!(matches!(
            a.authorize_sql("u", &stmt, "demo"),
            Decision::Deny {
                action: Action::ReadData,
                ..
            }
        ));
    }

    #[test]
    fn data_modifying_cte_requires_write() {
        let a = authz("grant u read demo.trips\n");
        let stmt =
            parse1("with deleted as (delete from demo.trips returning *) select * from deleted");
        assert!(matches!(
            a.authorize_sql("u", &stmt, "demo"),
            Decision::Deny {
                action: Action::WriteData,
                ..
            }
        ));
    }

    #[test]
    fn nonrecursive_cte_alias_only_enters_scope_after_its_definition() {
        // Inside its own non-recursive definition, `shadow` still resolves to
        // the base table. The outer reference resolves to the CTE and must not
        // add a second (or, critically, suppress the only) policy check.
        let stmt = parse1("with shadow as (select * from shadow) select * from shadow");
        assert_eq!(
            required_checks(&stmt, "demo").unwrap(),
            vec![(
                Action::ReadData,
                TableRef {
                    namespace: "demo".into(),
                    table: "shadow".into(),
                }
            )]
        );

        // Earlier aliases are visible to later definitions.
        let chained = parse1(
            "with first as (select * from source), \
             second as (select * from first) select * from second",
        );
        assert_eq!(
            required_checks(&chained, "demo").unwrap(),
            vec![(
                Action::ReadData,
                TableRef {
                    namespace: "demo".into(),
                    table: "source".into(),
                }
            )]
        );
    }

    #[test]
    fn recursive_cte_alias_is_visible_inside_its_definition() {
        let stmt = parse1(
            "with recursive nums(n) as \
             (values (1) union all select n + 1 from nums where n < 3) \
             select * from nums",
        );
        assert!(required_checks(&stmt, "demo").unwrap().is_empty());
    }

    #[test]
    fn update_subquery_requires_source_read() {
        let a = authz("grant u write demo.public\n");
        let stmt = parse1("update demo.public set value = (select value from secret.data limit 1)");
        assert!(matches!(
            a.authorize_sql("u", &stmt, "demo"),
            Decision::Deny {
                action: Action::ReadData,
                target: TableRef { namespace, table },
            } if namespace == "secret" && table == "data"
        ));
    }

    #[test]
    fn ctas_and_view_require_source_read() {
        let a = authz("");
        for sql in [
            "create table demo.copy as select * from secret.data",
            "create view demo.copy as select * from secret.data",
        ] {
            let stmt = parse1(sql);
            assert!(matches!(
                a.authorize_sql("u", &stmt, "demo"),
                Decision::Deny {
                    action: Action::ReadData,
                    ..
                }
            ));
        }
    }

    #[test]
    fn quoted_dots_and_case_match_engine_resolution() {
        let dotted = parse1("select * from \"secret.table\"");
        assert_eq!(
            required_checks(&dotted, "demo").unwrap(),
            vec![(
                Action::ReadData,
                TableRef {
                    namespace: "demo".into(),
                    table: "secret.table".into(),
                }
            )]
        );

        let dotted_ns = parse1("select * from \"a.b\".\"Mixed.Table\"");
        assert_eq!(
            required_checks(&dotted_ns, "demo").unwrap(),
            vec![(
                Action::ReadData,
                TableRef {
                    namespace: "a.b".into(),
                    table: "Mixed.Table".into(),
                }
            )]
        );

        let folded = parse1("select * from DEMO.Trips");
        assert_eq!(
            required_checks(&folded, "ignored").unwrap(),
            vec![(
                Action::ReadData,
                TableRef {
                    namespace: "demo".into(),
                    table: "trips".into(),
                }
            )]
        );
    }

    #[test]
    fn unknown_relation_bearing_statement_fails_closed() {
        let a = authz("grant u read demo.trips\n");
        let stmt = parse1("truncate table demo.trips");
        assert_eq!(
            a.authorize_sql("u", &stmt, "demo"),
            Decision::DenyUnsupported
        );
    }
}
