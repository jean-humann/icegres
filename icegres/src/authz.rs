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
    CopySource, ObjectName, ObjectNamePart, Statement, TableFactor, TableObject, Visit, Visitor,
};
use datafusion_postgres::pgwire::api::{ClientInfo, METADATA_USER};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use std::ops::ControlFlow;

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
    /// The statement cannot be safely mapped to the served catalog.
    Unsupported(String),
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
        let checks = match required_checks(stmt, default_namespace) {
            Ok(checks) => checks,
            Err(e) => return Decision::Unsupported(e.to_string()),
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
    matches!(ns, "pg_catalog" | "information_schema")
}

/// Enumerate every data access before any execution-capable planning call.
/// Unknown statement and relation forms fail closed when grants are enabled.
pub fn required_checks(stmt: &Statement, default_ns: &str) -> Result<Vec<(Action, TableRef)>> {
    let mut checks = Vec::new();
    match stmt {
        Statement::Query(q) => collect_reads(q.as_ref(), default_ns, &mut checks)?,
        Statement::Explain { statement, .. } => {
            checks.extend(required_checks(statement, default_ns)?);
        }
        Statement::Insert(insert) => {
            let TableObject::TableName(name) = &insert.table else {
                bail!("authorization does not support this INSERT target");
            };
            checks.push((
                Action::WriteData,
                object_name_to_ref(name, default_ns, Action::WriteData)?,
            ));
            collect_reads(stmt, default_ns, &mut checks)?;
        }
        Statement::Update { table, .. } => {
            let TableFactor::Table {
                name, args: None, ..
            } = &table.relation
            else {
                bail!("authorization requires a named UPDATE target");
            };
            checks.push((
                Action::WriteData,
                object_name_to_ref(name, default_ns, Action::WriteData)?,
            ));
            // Walk assignments, FROM, predicates and RETURNING as well as the target.
            collect_reads(stmt, default_ns, &mut checks)?;
        }
        Statement::Delete(del) => {
            let froms = match &del.from {
                sqlparser::ast::FromTable::WithFromKeyword(f)
                | sqlparser::ast::FromTable::WithoutKeyword(f) => f,
            };
            for table in froms {
                let TableFactor::Table {
                    name, args: None, ..
                } = &table.relation
                else {
                    bail!("authorization requires a named DELETE target");
                };
                checks.push((
                    Action::WriteData,
                    object_name_to_ref(name, default_ns, Action::WriteData)?,
                ));
            }
            for name in &del.tables {
                checks.push((
                    Action::WriteData,
                    object_name_to_ref(name, default_ns, Action::WriteData)?,
                ));
            }
            collect_reads(stmt, default_ns, &mut checks)?;
        }
        Statement::Copy {
            source, to, target, ..
        } => {
            use sqlparser::ast::CopyTarget;
            anyhow::ensure!(
                matches!(target, CopyTarget::Stdin | CopyTarget::Stdout),
                "authorization permits COPY only through STDIN/STDOUT"
            );
            match source {
                CopySource::Table { table_name, .. } => checks.push((
                    if *to {
                        Action::ReadData
                    } else {
                        Action::WriteData
                    },
                    object_name_to_ref(
                        table_name,
                        default_ns,
                        if *to {
                            Action::ReadData
                        } else {
                            Action::WriteData
                        },
                    )?,
                )),
                CopySource::Query(q) => collect_reads(q.as_ref(), default_ns, &mut checks)?,
            }
        }
        Statement::CreateTable(create) => {
            anyhow::ensure!(
                !create.external
                    && create.location.is_none()
                    && create.like.is_none()
                    && create.clone.is_none()
                    && create.inherits.is_none(),
                "authorization does not support external or inherited CREATE TABLE"
            );
            anyhow::ensure!(
                create.query.is_some(),
                "authorization currently supports CREATE TABLE only with an explicit SELECT source"
            );
            checks.push((
                Action::WriteData,
                object_name_to_ref(&create.name, default_ns, Action::WriteData)?,
            ));
            collect_reads(stmt, default_ns, &mut checks)?;
        }
        Statement::Drop {
            names,
            object_type: sqlparser::ast::ObjectType::Table,
            ..
        } => {
            for name in names {
                checks.push((
                    Action::DropTable,
                    object_name_to_ref(name, default_ns, Action::DropTable)?,
                ));
            }
        }
        Statement::Set(set) => {
            // Write hooks resolve bare names in the fixed served namespace.
            // Until settings are session-isolated, deny changes that can make
            // authorization and execution resolve the same SQL differently.
            anyhow::ensure!(!changes_resolution(set),
                "changing catalog/schema/parser resolution is not supported with table grants or constraints");
            collect_reads(stmt, default_ns, &mut checks)?;
        }
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
        | Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. } => collect_reads(stmt, default_ns, &mut checks)?,
        _ => bail!("statement form is not supported with table grants"),
    }
    // Only metadata READS are exempt. A write to a metadata-named schema
    // must never bypass grants through this exception.
    checks.retain(|(action, t)| *action != Action::ReadData || !is_system_schema(&t.namespace));
    Ok(checks)
}

/// Whether `stmt` is side-effect-free and therefore admissible on a
/// read-only pgwire or Flight listener.
///
/// This is a distinct concern from [`required_checks`], which enumerates the
/// ReBAC data-plane checks a statement needs. A permitted write must still
/// be refused on a read-only endpoint. This predicate is therefore
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
        Statement::Copy {
            source,
            to: true,
            target: sqlparser::ast::CopyTarget::Stdout,
            ..
        } => match source {
            CopySource::Table { .. } => true,
            CopySource::Query(q) => query_is_read_only(q),
        },
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
    struct ReadQueries;
    impl Visitor for ReadQueries {
        type Break = ();
        fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<()> {
            if !set_expr_is_read_only(&query.body)
                || !query.pipe_operators.is_empty()
                || !query.locks.is_empty()
            {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    query.visit(&mut ReadQueries).is_continue()
}

/// Whether a query body's set-expression is a pure read. `Insert`/`Update`/
/// `Delete`/`Merge` embedded in a query body are writes; fail closed on any
/// unrecognized future variant.
fn set_expr_is_read_only(body: &sqlparser::ast::SetExpr) -> bool {
    use sqlparser::ast::SetExpr;
    match body {
        SetExpr::Select(select) => select.into.is_none(),
        SetExpr::Values(_) => true,
        SetExpr::Query(q) => query_is_read_only(q),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_is_read_only(left) && set_expr_is_read_only(right)
        }
        _ => false,
    }
}

/// Walk all nested relations, including DML expression subqueries. Unknown
/// table-producing forms are denied instead of silently skipping their input.
fn collect_reads<T: Visit>(
    node: &T,
    default_ns: &str,
    out: &mut Vec<(Action, TableRef)>,
) -> Result<()> {
    struct Scope {
        visible: HashSet<String>,
        // A nonrecursive CTE becomes visible only after its definition.
        // Otherwise WITH t AS (SELECT * FROM t) could hide a base-table read.
        definitions: HashMap<usize, String>,
    }
    struct Reads<'a> {
        default_ns: &'a str,
        out: &'a mut Vec<(Action, TableRef)>,
        scopes: Vec<Scope>,
    }
    impl Visitor for Reads<'_> {
        type Break = anyhow::Error;
        fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
            if !query_is_read_only(query) || !query.pipe_operators.is_empty() {
                return ControlFlow::Break(anyhow::anyhow!(
                    "authorization does not support data-modifying query bodies"
                ));
            }
            let mut scope = Scope {
                visible: HashSet::new(),
                definitions: HashMap::new(),
            };
            if let Some(with) = &query.with {
                // DataFusion resolves a recursive CTE's seed before exposing
                // its self-reference, and non-UNION bodies are nonrecursive.
                // Until we model that exact scope, fail closed.
                if with.recursive {
                    return ControlFlow::Break(anyhow::anyhow!(
                        "recursive CTEs are not supported with table grants"
                    ));
                }
                for cte in &with.cte_tables {
                    let name = normalized_ident(&cte.alias.name);
                    scope
                        .definitions
                        .insert(cte.query.as_ref() as *const _ as usize, name);
                }
            }
            self.scopes.push(scope);
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
            self.scopes.pop();
            if let Some(parent) = self.scopes.last_mut() {
                if let Some(name) = parent.definitions.get(&(query as *const _ as usize)) {
                    parent.visible.insert(name.clone());
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            match factor {
                TableFactor::Table { args: None, .. }
                | TableFactor::Derived { .. }
                | TableFactor::NestedJoin { .. }
                | TableFactor::UNNEST { .. } => ControlFlow::Continue(()),
                _ => ControlFlow::Break(anyhow::anyhow!(
                    "table-producing function/form is not supported with table grants"
                )),
            }
        }
        fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
            if let [ObjectNamePart::Identifier(ident)] = name.0.as_slice() {
                let normalized = normalized_ident(ident);
                if self
                    .scopes
                    .iter()
                    .rev()
                    .any(|scope| scope.visible.contains(&normalized))
                {
                    return ControlFlow::Continue(());
                }
            }
            match object_name_to_ref(name, self.default_ns, Action::ReadData) {
                Ok(t) => {
                    self.out.push((Action::ReadData, t));
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(e),
            }
        }
    }
    match node.visit(&mut Reads {
        default_ns,
        out,
        scopes: Vec::new(),
    }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

fn normalized_ident(ident: &sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

fn object_name_to_ref(name: &ObjectName, default_ns: &str, action: Action) -> Result<TableRef> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => Ok(normalized_ident(ident)),
            _ => anyhow::bail!("unsupported table identifier"),
        })
        .collect::<Result<_>>()?;
    let (namespace, table) = match parts.as_slice() {
        [table] => (default_ns.to_string(), table.clone()),
        [namespace, table] => (namespace.clone(), table.clone()),
        [catalog, namespace, table] if catalog == crate::context::CATALOG_NAME => {
            (namespace.clone(), table.clone())
        }
        _ => bail!("table identifier must resolve in the served Icegres catalog"),
    };
    anyhow::ensure!(
        !namespace.is_empty() && !table.is_empty(),
        "empty table identifier"
    );
    if action != Action::ReadData {
        // Write hooks resolve targets literally, while read providers interpret
        // suffixes. Reject ambiguous write targets instead of granting a write
        // on a different base table.
        anyhow::ensure!(!table.contains('$') && !table.rsplit_once('@').is_some_and(|(base, suffix)|
            !base.is_empty() && suffix.parse::<i64>().is_ok()),
            "metadata/snapshot references cannot be write targets with table grants");
        return Ok(TableRef { namespace, table });
    }
    // Mirror CachingSchemaProvider and iceberg-datafusion exactly. A literal
    // name such as "allowed@secret" is not a snapshot reference.
    let table = if let Some((base, suffix)) = table.split_once('$') {
        anyhow::ensure!(
            !base.is_empty() && iceberg::inspect::MetadataTableType::try_from(suffix).is_ok(),
            "unknown metadata-table suffix"
        );
        base.to_string()
    } else if let Some((base, suffix)) = table.rsplit_once('@') {
        if !base.is_empty() && suffix.parse::<i64>().is_ok() {
            base.to_string()
        } else {
            table
        }
    } else {
        table
    };
    Ok(TableRef { namespace, table })
}

fn changes_resolution(set: &sqlparser::ast::Set) -> bool {
    fn key(name: &ObjectName) -> bool {
        let parts: Option<Vec<_>> = name
            .0
            .iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
                _ => None,
            })
            .collect();
        let Some(parts) = parts else {
            return true;
        };
        matches!(
            parts.join(".").as_str(),
            "search_path"
                | "datafusion.catalog.default_schema"
                | "datafusion.catalog.default_catalog"
                | "datafusion.sql_parser.enable_ident_normalization"
                | "datafusion.sql_parser.dialect"
        )
    }
    use sqlparser::ast::Set;
    match set {
        Set::SingleAssignment { variable, .. } => key(variable),
        Set::ParenthesizedAssignments { variables, .. } => variables.iter().any(key),
        Set::MultipleAssignments { assignments } => assignments.iter().any(|a| key(&a.name)),
        _ => false,
    }
}

/// Refuse a changed default namespace because the write hooks currently use
/// the served catalog/schema even when a DataFusion session setting changes.
pub fn check_namespace(ctx: &SessionContext, default_ns: &str) -> Result<()> {
    let state = ctx.state();
    let catalog = &state.config_options().catalog;
    let parser = &state.config_options().sql_parser;
    anyhow::ensure!(parser.enable_ident_normalization
        && matches!(parser.dialect, datafusion::common::config::Dialect::Generic
            | datafusion::common::config::Dialect::PostgreSQL),
        "changed SQL identifier normalization or dialect is not supported with table grants or constraints");
    anyhow::ensure!(
        catalog.default_catalog == crate::context::CATALOG_NAME
            && catalog.default_schema == default_ns,
        "changed catalog/schema resolution is not supported with table grants"
    );
    Ok(())
}

/// Shared authorizer handle used by the hook and the Flight SQL path.
pub type SharedAuthorizer = Arc<dyn Authorizer>;

/// Build the SQLSTATE 42501 (insufficient_privilege) error for a denied
/// statement, in the shape Postgres clients expect.
/// Human-readable permission-denied message, shared by the pgwire error
/// (`deny_error`) and the Flight SQL `Status::permission_denied` so both wire
/// protocols report a denial identically.
pub fn deny_message(principal: &str, action: Action, target: &TableRef) -> String {
    let verb = match action {
        Action::ReadData => "SELECT",
        Action::WriteData => "write (INSERT/UPDATE/DELETE)",
        Action::DropTable => "DROP",
    };
    format!("permission denied: role \"{principal}\" cannot {verb} on {target}")
}

pub fn deny_error(principal: &str, action: Action, target: &TableRef) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42501".to_string(),
        deny_message(principal, action, target),
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
        ctx: &SessionContext,
    ) -> Option<PgWireError> {
        if let Err(e) = check_namespace(ctx, &self.default_namespace) {
            return Some(policy_error("42501", &e.to_string()));
        }
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
            Decision::Unsupported(reason) => Some(policy_error("42501", &reason)),
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
        ctx: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        self.gate(statement, client, ctx).map(Err)
    }

    async fn handle_extended_parse_query(
        &self,
        sql: &Statement,
        ctx: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        self.gate(sql, client, ctx).map(Err)
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _plan: &LogicalPlan,
        _params: &ParamValues,
        ctx: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        self.gate(statement, client, ctx).map(Err)
    }
}

/// Constraint enforcement must intercept every statement that can write.
/// Wrappers and alternative write forms cannot fall through to DataFusion's
/// append path, which does not know the listener's constraint policy.
pub fn check_constraint_statement(
    stmt: &Statement,
    ctx: &SessionContext,
    default_ns: &str,
) -> Result<()> {
    check_namespace(ctx, default_ns)?;
    if matches!(stmt, Statement::Set(_)) {
        required_checks(stmt, default_ns)?;
    }
    anyhow::ensure!(
        is_read_only(stmt)
            || matches!(
                stmt,
                Statement::Insert(_) | Statement::Update { .. } | Statement::Delete(_)
            ),
        "--enforce-pk permits writes only as direct INSERT/UPDATE/DELETE statements"
    );
    struct QueryReads;
    impl Visitor for QueryReads {
        type Break = ();
        fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<()> {
            if query_is_read_only(query) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }
    }
    anyhow::ensure!(
        stmt.visit(&mut QueryReads).is_continue(),
        "data-modifying query bodies are not supported with --enforce-pk"
    );
    Ok(())
}

pub struct ConstraintHook;

impl ConstraintHook {
    fn gate(stmt: &Statement, ctx: &SessionContext) -> Option<PgWireError> {
        check_constraint_statement(stmt, ctx, crate::context::DEFAULT_SCHEMA)
            .err()
            .map(|e| policy_error("0A000", &e.to_string()))
    }
}

#[async_trait]
impl QueryHook for ConstraintHook {
    async fn handle_simple_query(
        &self,
        stmt: &Statement,
        ctx: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        Self::gate(stmt, ctx).map(Err)
    }
    async fn handle_extended_parse_query(
        &self,
        stmt: &Statement,
        ctx: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        Self::gate(stmt, ctx).map(Err)
    }
    async fn handle_extended_query(
        &self,
        stmt: &Statement,
        _plan: &LogicalPlan,
        _params: &ParamValues,
        ctx: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        Self::gate(stmt, ctx).map(Err)
    }
}

fn policy_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.into(),
    )))
}

/// Read-only enforcement is independent of optional table-grant policy and
/// runs before any hook that can buffer, rewrite, or execute a statement.
pub struct ReadOnlyHook;

impl ReadOnlyHook {
    fn gate(statement: &Statement) -> Option<PgWireError> {
        (!is_read_only(statement)).then(|| {
            policy_error(
                "25006",
                "cannot execute a write on a read-only Icegres server",
            )
        })
    }
}

#[async_trait]
impl QueryHook for ReadOnlyHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _ctx: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        Self::gate(statement).map(Err)
    }
    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _ctx: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        Self::gate(statement).map(Err)
    }
    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _plan: &LogicalPlan,
        _params: &ParamValues,
        _ctx: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        Self::gate(statement).map(Err)
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
            .unwrap_or_else(|error| panic!("failed to parse {sql:?}: {error}"))
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
    fn nested_access_and_identifiers_are_checked() {
        let a = authz("grant u write demo.allowed\ngrant u write demo.leak\n");
        for sql in [
            "EXPLAIN ANALYZE SELECT * FROM demo.secret",
            "CREATE TABLE demo.leak AS SELECT * FROM demo.secret",
            "SELECT * FROM demo.\"secret.with.dots\"",
            "UPDATE demo.allowed SET id = (SELECT max(id) FROM demo.secret)",
            "DELETE FROM demo.allowed WHERE id IN (SELECT id FROM demo.secret)",
            "SELECT * FROM other.demo.allowed",
            "SELECT * FROM \"PG_CATALOG\".secret",
            "SELECT * FROM pg_temp.secret",
            "CREATE VIEW demo.leak AS SELECT * FROM demo.secret",
            "SELECT * FROM read_parquet('secret.parquet')",
            "SET datafusion.catalog.default_schema = 'secret'",
            "SET datafusion.sql_parser.enable_ident_normalization = false",
            "SET \"datafusion\".\"sql_parser\".\"dialect\" = 'mysql'",
            "WITH RECURSIVE secret AS (SELECT * FROM demo.secret) SELECT * FROM secret",
        ] {
            assert_ne!(
                a.authorize_sql("u", &parse1(sql), "demo"),
                Decision::Allow,
                "{sql}"
            );
        }
        assert_eq!(
            a.authorize_sql("u", &parse1("SELECT * FROM DEMO.ALLOWED"), "demo"),
            Decision::Allow
        );
        assert_eq!(
            a.authorize_sql("u", &parse1("SELECT * FROM icegres.demo.allowed"), "demo"),
            Decision::Allow
        );
        assert_eq!(
            a.authorize_sql(
                "u",
                &parse1("EXPLAIN ANALYZE SELECT * FROM demo.allowed"),
                "demo"
            ),
            Decision::Allow
        );
        let dotted = authz("grant u read demo.secret.with.dots\n");
        assert_eq!(
            dotted.authorize_sql(
                "u",
                &parse1("SELECT * FROM demo.\"secret.with.dots\""),
                "demo"
            ),
            Decision::Allow
        );
    }

    #[tokio::test]
    async fn wire_hooks_deny_before_datafusion_executes_wrappers() {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::catalog::MemTable;
        use datafusion::common::TableReference;
        use datafusion::execution::context::SessionConfig;
        use datafusion_postgres::pgwire::api::DefaultClient;

        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_default_catalog_and_schema("icegres", "demo"),
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![42]))],
        )
        .unwrap();
        for name in ["secret", "secret.with.dots", "allowed", "allowed@secret"] {
            ctx.register_table(
                TableReference::partial("demo", name),
                Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap()),
            )
            .unwrap();
        }
        let hook = AuthzHook::new(
            Arc::new(authz(
                "grant u write demo.allowed\ngrant u write demo.leak\n",
            )),
            "demo".into(),
        );
        let mut client = DefaultClient::<()>::new("127.0.0.1:5432".parse().unwrap(), false);
        client
            .metadata_mut()
            .insert(METADATA_USER.into(), "u".into());
        let dummy = ctx.sql("SELECT 1").await.unwrap().logical_plan().clone();
        for sql in [
            "SELECT * FROM demo.secret",
            "EXPLAIN ANALYZE SELECT * FROM demo.secret",
            "CREATE TABLE demo.leak AS SELECT * FROM demo.secret",
            "SELECT * FROM demo.\"secret.with.dots\"",
            "SELECT * FROM demo.\"allowed@secret\"",
            "WITH RECURSIVE secret AS (SELECT * FROM secret) SELECT * FROM secret",
            "WITH RECURSIVE secret AS (SELECT * FROM demo.secret) SELECT * FROM secret",
        ] {
            let stmt = parse1(sql);
            assert!(
                matches!(
                    hook.handle_simple_query(&stmt, &ctx, &mut client).await,
                    Some(Err(_))
                ),
                "{sql}"
            );
            assert!(
                matches!(
                    hook.handle_extended_parse_query(&stmt, &ctx, &client).await,
                    Some(Err(_))
                ),
                "{sql}"
            );
            assert!(
                matches!(
                    hook.handle_extended_query(
                        &stmt,
                        &dummy,
                        &ParamValues::List(vec![]),
                        &ctx,
                        &mut client
                    )
                    .await,
                    Some(Err(_))
                ),
                "{sql}"
            );
        }
        assert!(ctx.table("demo.leak").await.is_err());
        // The qualified source is an executable denied read. An unqualified
        // self-reference instead fails during DataFusion's provider preload,
        // so it does not establish an authorization bypass on its own.
        let unguarded = ctx
            .sql("WITH RECURSIVE secret AS (SELECT * FROM demo.secret) SELECT * FROM secret")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(unguarded.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        let mut changed = SessionConfig::new().with_default_catalog_and_schema("icegres", "demo");
        changed.options_mut().sql_parser.enable_ident_normalization = false;
        assert!(check_namespace(&SessionContext::new_with_config(changed), "demo").is_err());
        let allowed = parse1("EXPLAIN ANALYZE SELECT * FROM demo.allowed");
        assert!(hook
            .handle_simple_query(&allowed, &ctx, &mut client)
            .await
            .is_none());
        ctx.sql(&allowed.to_string())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        // Fail closed if any other path changed the planner's namespace.
        let other = SessionContext::new();
        assert!(matches!(
            hook.handle_simple_query(&allowed, &other, &mut client)
                .await,
            Some(Err(_))
        ));
    }

    #[tokio::test]
    async fn read_only_hook_covers_simple_parse_and_execute() {
        use datafusion_postgres::pgwire::api::DefaultClient;
        let ctx = SessionContext::new();
        let mut client = DefaultClient::<()>::new("127.0.0.1:5432".parse().unwrap(), false);
        let plan = ctx.sql("SELECT 1").await.unwrap().logical_plan().clone();
        let hook = ReadOnlyHook;
        assert!(is_read_only(&parse1("COPY demo.t TO STDOUT")));
        assert!(is_read_only(&parse1(
            "COPY (SELECT * FROM demo.t) TO STDOUT"
        )));
        assert!(!is_read_only(&parse1("COPY demo.t TO '/tmp/export'")));
        for sql in [
            "INSERT INTO demo.t VALUES (1)",
            "UPDATE demo.t SET id=1",
            "DELETE FROM demo.t",
            "CREATE TABLE demo.leak AS SELECT 1",
            "DROP TABLE demo.t",
            "SELECT 1 INTO demo.leak",
            "WITH x AS (SELECT 1) INSERT INTO demo.t SELECT * FROM x",
            "EXPLAIN ANALYZE INSERT INTO demo.t VALUES (1)",
        ] {
            let stmt = parse1(sql);
            assert!(
                matches!(
                    hook.handle_simple_query(&stmt, &ctx, &mut client).await,
                    Some(Err(_))
                ),
                "{sql}"
            );
            assert!(
                matches!(
                    hook.handle_extended_parse_query(&stmt, &ctx, &client).await,
                    Some(Err(_))
                ),
                "{sql}"
            );
            assert!(
                matches!(
                    hook.handle_extended_query(
                        &stmt,
                        &plan,
                        &ParamValues::List(vec![]),
                        &ctx,
                        &mut client
                    )
                    .await,
                    Some(Err(_))
                ),
                "{sql}"
            );
        }
        assert!(hook
            .handle_simple_query(&parse1("SELECT 1"), &ctx, &mut client)
            .await
            .is_none());
    }
    #[test]
    fn cte_aliases_do_not_hide_denied_base_relations() {
        let a = authz("grant u read demo.allowed\n");
        for sql in [
            "WITH x AS (SELECT * FROM demo.allowed) SELECT * FROM x",
            "WITH x AS (SELECT * FROM demo.allowed), y AS (SELECT * FROM x) SELECT * FROM y",
            "WITH x AS (SELECT * FROM demo.allowed) SELECT * FROM (WITH x AS (SELECT * FROM x) SELECT * FROM x) z",
        ] {
            assert_eq!(a.authorize_sql("u", &parse1(sql), "demo"), Decision::Allow, "{sql}");
        }
        for sql in [
            "WITH secret AS (SELECT * FROM secret) SELECT * FROM secret",
            "WITH allowed AS (SELECT * FROM demo.secret) SELECT * FROM allowed",
            "WITH x AS (SELECT * FROM demo.allowed) SELECT * FROM (WITH x AS (SELECT * FROM demo.secret) SELECT * FROM x) z",
        ] {
            assert_ne!(a.authorize_sql("u", &parse1(sql), "demo"), Decision::Allow, "{sql}");
        }
    }
    #[test]
    fn quoted_and_unquoted_unicode_identifiers_follow_planner_normalization() {
        let a = authz("grant u read demo.Å\n");
        assert_eq!(
            a.authorize_sql("u", &parse1("SELECT * FROM demo.\"Å\""), "demo"),
            Decision::Allow
        );
        assert_ne!(
            a.authorize_sql("u", &parse1("SELECT * FROM demo.Å"), "demo"),
            Decision::Allow
        );
    }
    #[test]
    fn suffixes_match_provider_resolution() {
        let a = authz("grant u read demo.allowed\n");
        assert_ne!(
            a.authorize_sql(
                "u",
                &parse1("SELECT * FROM demo.\"allowed@secret\""),
                "demo"
            ),
            Decision::Allow
        );
        assert_ne!(
            a.authorize_sql(
                "u",
                &parse1("SELECT * FROM demo.\"allowed@secret@123\""),
                "demo"
            ),
            Decision::Allow
        );
        assert_eq!(
            a.authorize_sql("u", &parse1("SELECT * FROM demo.\"allowed@123\""), "demo"),
            Decision::Allow
        );
        assert_eq!(
            a.authorize_sql(
                "u",
                &parse1("SELECT * FROM demo.\"allowed$snapshots\""),
                "demo"
            ),
            Decision::Allow
        );
        assert_ne!(
            a.authorize_sql(
                "u",
                &parse1("SELECT * FROM demo.\"allowed$unknown\""),
                "demo"
            ),
            Decision::Allow
        );
    }

    #[tokio::test]
    async fn constrained_writes_cannot_escape_through_query_wrappers() {
        use datafusion::execution::context::SessionConfig;
        use datafusion_postgres::pgwire::api::DefaultClient;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_default_catalog_and_schema("icegres", "demo"),
        );
        let mut client = DefaultClient::<()>::new("127.0.0.1:5432".parse().unwrap(), false);
        let plan = ctx.sql("SELECT 1").await.unwrap().logical_plan().clone();
        let hook = ConstraintHook;
        for sql in [
            "EXPLAIN ANALYZE INSERT INTO demo.t VALUES (1)",
            "WITH x AS (SELECT 1) INSERT INTO demo.t SELECT * FROM x",
            "CREATE TABLE demo.leak AS SELECT 1",
            "COPY demo.t FROM STDIN;\n\\.",
            "SET datafusion.catalog.default_schema = 'other'",
        ] {
            let stmt = parse1(sql);
            assert!(
                matches!(
                    hook.handle_simple_query(&stmt, &ctx, &mut client).await,
                    Some(Err(_))
                ),
                "{sql}"
            );
            assert!(
                matches!(
                    hook.handle_extended_parse_query(&stmt, &ctx, &client).await,
                    Some(Err(_))
                ),
                "{sql}"
            );
            assert!(
                matches!(
                    hook.handle_extended_query(
                        &stmt,
                        &plan,
                        &ParamValues::List(vec![]),
                        &ctx,
                        &mut client
                    )
                    .await,
                    Some(Err(_))
                ),
                "{sql}"
            );
        }
        for sql in [
            "SELECT 1",
            "INSERT INTO demo.t VALUES (1)",
            "UPDATE demo.t SET id=2",
            "DELETE FROM demo.t",
        ] {
            assert!(
                hook.handle_simple_query(&parse1(sql), &ctx, &mut client)
                    .await
                    .is_none(),
                "{sql}"
            );
        }
    }
    #[test]
    fn read_suffix_permissions_do_not_authorize_literal_write_targets() {
        let a = authz("grant u write demo.allowed\ngrant u drop demo.allowed\n");
        for sql in [
            "UPDATE demo.\"allowed@123\" SET id=2",
            "DELETE FROM demo.\"allowed@123\"",
            "INSERT INTO demo.\"allowed@123\" VALUES (1)",
            "DROP TABLE demo.\"allowed@123\"",
            "CREATE TABLE demo.\"allowed@123\" AS SELECT 1",
            "UPDATE demo.\"allowed$snapshots\" SET id=2",
            "DELETE FROM demo.\"allowed@secret\"",
        ] {
            assert_ne!(
                a.authorize_sql("u", &parse1(sql), "demo"),
                Decision::Allow,
                "{sql}"
            );
        }
    }
}
