use super::*;
use datafusion::prelude::SessionContext;
use iceberg::memory::MemoryCatalogBuilder;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{CatalogBuilder, ErrorKind, TableCreation};

async fn two_table_context() -> (Arc<dyn Catalog>, SessionContext, NamespaceIdent) {
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([("warehouse".to_string(), "memory://warehouse".to_string())]),
            )
            .await
            .unwrap(),
    );
    let namespace = NamespaceIdent::new("drop_regression".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    for name in ["survivor", "removed"] {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(name.to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap();
    }
    let context =
        crate::context::build_session_context_with(catalog.clone(), Some(1), None, None, 0)
            .await
            .unwrap();
    (catalog, context, namespace)
}

#[tokio::test]
async fn external_drop_does_not_poison_unrelated_catalog_queries() {
    let (catalog, context, namespace) = two_table_context().await;
    datafusion_postgres::datafusion_pg_catalog::pg_catalog::setup_pg_catalog(
        &context,
        crate::context::CATALOG_NAME,
        Arc::new(datafusion_postgres::auth::AuthManager::default()),
    )
    .unwrap();
    crate::compat::install_coherent_pg_catalog(&context, crate::context::CATALOG_NAME)
        .await
        .unwrap();
    let schema = context
        .catalog(crate::context::CATALOG_NAME)
        .unwrap()
        .schema("drop_regression")
        .unwrap();
    assert!(schema
        .table_names()
        .contains(&"removed$snapshots".to_string()));

    // The provider inventory was captured with both tables. A different
    // catalog client removes one before the first ORM catalog materialization.
    catalog
        .drop_table(&TableIdent::new(namespace, "removed".to_string()))
        .await
        .unwrap();
    context
        .sql(
            "SELECT t.oid, t.typarray FROM pg_catalog.pg_type t \
        JOIN pg_catalog.pg_namespace ns ON t.typnamespace = ns.oid \
        WHERE t.typname = 'hstore'",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    context
        .sql("SELECT count(*) FROM drop_regression.survivor")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    context
        .sql("SELECT * FROM drop_regression.\"survivor$snapshots\"")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(!schema.table_exist("removed"));
    assert!(!schema
        .table_names()
        .iter()
        .any(|name| name.starts_with("removed")));
    let error = context
        .sql("SELECT * FROM drop_regression.removed")
        .await
        .expect_err("the dropped table must not resolve");
    assert!(error.to_string().contains("not found"), "{error}");
    assert!(schema.table("removed$snapshots").await.unwrap().is_none());
    assert!(schema.table("removed@123").await.unwrap().is_none());
}

#[derive(Debug)]
struct MetadataFailure {
    inner: Arc<dyn SchemaProvider>,
    kind: ErrorKind,
    message: &'static str,
}

#[async_trait]
impl SchemaProvider for MetadataFailure {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }
    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if name.contains('$') {
            Err(to_datafusion_error(iceberg::Error::new(
                self.kind,
                self.message,
            )))
        } else {
            self.inner.table(name).await
        }
    }
}

#[tokio::test]
async fn catalog_errors_do_not_remove_registered_tables() {
    let (catalog, context, namespace) = two_table_context().await;
    let inner = context
        .catalog(crate::context::CATALOG_NAME)
        .unwrap()
        .schema("drop_regression")
        .unwrap();
    // REST non-404 responses become DataInvalid, including server-provided
    // messages that happen to contain the exact not-found text.
    for (kind, message) in [
        (ErrorKind::DataInvalid, "HTTP 401 Unauthorized"),
        (ErrorKind::DataInvalid, "HTTP 403 Forbidden"),
        (ErrorKind::DataInvalid, "HTTP 503 Service Unavailable"),
        (
            ErrorKind::DataInvalid,
            "Tried to load a table that does not exist",
        ),
        (ErrorKind::Unexpected, "connection reset by peer"),
        (
            ErrorKind::Unexpected,
            "catalog load_table timed out after 5000 ms",
        ),
    ] {
        let schema = CachingSchemaProvider::try_new(
            Arc::new(MetadataFailure {
                inner: inner.clone(),
                kind,
                message,
            }),
            catalog.clone(),
            namespace.clone(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
        let before = schema.table_names();
        let error = schema
            .table("removed$snapshots")
            .await
            .expect_err("catalog failures must remain visible");
        assert!(error.to_string().contains(message));
        assert!(schema.table_exist("removed"));
        assert_eq!(schema.table_names(), before);
        assert!(schema.table("removed").await.unwrap().is_some());
    }
}

#[tokio::test]
async fn pinned_rest_404_mapping_removes_only_the_missing_table() {
    let (catalog, context, namespace) = two_table_context().await;
    let inner = context
        .catalog(crate::context::CATALOG_NAME)
        .unwrap()
        .schema("drop_regression")
        .unwrap();
    let schema = CachingSchemaProvider::try_new(
        Arc::new(MetadataFailure {
            inner,
            kind: ErrorKind::Unexpected,
            message: "Tried to load a table that does not exist",
        }),
        catalog,
        namespace,
        None,
        None,
        false,
        None,
    )
    .await
    .unwrap();
    assert!(schema.table("removed$snapshots").await.unwrap().is_none());
    assert!(!schema.table_exist("removed"));
    assert!(schema.table("survivor").await.unwrap().is_some());
}

#[tokio::test]
async fn confirmed_drop_cannot_fall_back_to_a_cached_snapshot() {
    let (catalog, context, namespace) = two_table_context().await;
    let schema = context
        .catalog(crate::context::CATALOG_NAME)
        .unwrap()
        .schema("drop_regression")
        .unwrap();
    let delegate = schema.table("removed").await.unwrap().unwrap();
    let ident = TableIdent::new(namespace, "removed".to_string());
    let provider = CachingTableProvider::new(
        catalog.clone(),
        ident.clone(),
        delegate,
        None,
        None,
        Some(Arc::new(TableFreshness::new())),
        None,
    );
    provider.load_current(LoadPath::Scan).await.unwrap();
    assert!(provider.cached.read().unwrap().is_some());
    assert!(provider.plan_cache_version().is_some());
    catalog.drop_table(&ident).await.unwrap();
    // A background refresh must revoke the previously fresh snapshot too.
    assert!(provider.refresh().await.is_err());
    assert!(provider.cached.read().unwrap().is_none());
    assert!(provider.plan_cache_version().is_none());
    assert!(provider.fresh_metadata().is_none());
    // Freshness mode otherwise permits stale fallback on catalog outages.
    let error = provider
        .current_provider()
        .await
        .expect_err("known deletion must never serve the retained snapshot");
    assert!(table_is_missing(&error), "{error}");
}

#[tokio::test]
async fn drop_filter_preserves_literal_dollar_and_at_names() {
    let (catalog, _, namespace) = two_table_context().await;
    let survivor = catalog
        .load_table(&TableIdent::new(namespace.clone(), "survivor".to_string()))
        .await
        .unwrap();
    for name in ["removed$other", "removed$snapshots", "removed@123"] {
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(name.to_string())
                    .schema(survivor.metadata().current_schema().as_ref().clone())
                    .build(),
            )
            .await
            .unwrap();
    }
    let context = crate::context::build_session_context_with(catalog, Some(1), None, None, 0)
        .await
        .unwrap();
    let schema = context
        .catalog(crate::context::CATALOG_NAME)
        .unwrap()
        .schema("drop_regression")
        .unwrap();
    let caching = schema
        .as_any()
        .downcast_ref::<CachingSchemaProvider>()
        .unwrap();
    caching.forget_dropped_table("removed", caching.registration_generation("removed"));
    for name in [
        "removed$other",
        "removed$snapshots",
        "removed@123",
        "removed$other$snapshots",
        "removed$snapshots$manifests",
    ] {
        assert!(
            !caching.was_dropped(name),
            "literal physical table hidden: {name}"
        );
        assert!(caching.table_names().contains(&name.to_string()));
    }
    assert!(caching.was_dropped("removed$manifests"));
    assert!(caching.was_dropped("removed@456"));
}

#[tokio::test]
async fn successful_explicit_registration_clears_drop_tombstone() {
    use datafusion::catalog::MemorySchemaProvider;
    let (catalog, context, namespace) = two_table_context().await;
    let upstream = context
        .catalog(crate::context::CATALOG_NAME)
        .unwrap()
        .schema("drop_regression")
        .unwrap();
    let table = upstream.table("removed").await.unwrap().unwrap();
    let inner = Arc::new(MemorySchemaProvider::new());
    inner
        .register_table("removed".to_string(), table.clone())
        .unwrap();
    let caching =
        CachingSchemaProvider::try_new(inner.clone(), catalog, namespace, None, None, false, None)
            .await
            .unwrap();
    caching.forget_dropped_table("removed", caching.registration_generation("removed"));
    assert!(caching.was_dropped("removed"));
    // Failed registration must not resurrect a tombstoned entry.
    assert!(caching
        .register_table("removed".to_string(), table.clone())
        .is_err());
    assert!(caching.was_dropped("removed"));
    inner.deregister_table("removed").unwrap();
    caching
        .register_table("removed".to_string(), table)
        .unwrap();
    assert!(!caching.was_dropped("removed"));
    assert!(caching.table("removed").await.unwrap().is_some());
}

#[derive(Debug)]
struct PausedLoadCatalog {
    inner: Arc<dyn Catalog>,
    pause_next: AtomicBool,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl Catalog for PausedLoadCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        name: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.create_namespace(name, properties).await
    }
    async fn get_namespace(&self, name: &NamespaceIdent) -> iceberg::Result<iceberg::Namespace> {
        self.inner.get_namespace(name).await
    }
    async fn namespace_exists(&self, name: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(name).await
    }
    async fn update_namespace(
        &self,
        name: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(name, properties).await
    }
    async fn drop_namespace(&self, name: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(name).await
    }
    async fn list_tables(&self, name: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(name).await
    }
    async fn create_table(
        &self,
        name: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(name, creation).await
    }
    async fn load_table(&self, ident: &TableIdent) -> iceberg::Result<Table> {
        let pause = self.pause_next.swap(false, Ordering::AcqRel);
        let result = self.inner.load_table(ident).await;
        if pause {
            self.started.notify_one();
            self.release.notified().await;
        }
        result
    }
    async fn drop_table(&self, ident: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(ident).await
    }
    async fn table_exists(&self, ident: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(ident).await
    }
    async fn rename_table(&self, from: &TableIdent, to: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(from, to).await
    }
    async fn register_table(&self, ident: &TableIdent, location: String) -> iceberg::Result<Table> {
        self.inner.register_table(ident, location).await
    }
    async fn update_table(&self, commit: iceberg::TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }
}

#[tokio::test]
async fn pre_drop_catalog_load_cannot_reinstall_a_deleted_snapshot() {
    let (catalog, context, namespace) = two_table_context().await;
    let schema = context
        .catalog(crate::context::CATALOG_NAME)
        .unwrap()
        .schema("drop_regression")
        .unwrap();
    let delegate = schema.table("removed").await.unwrap().unwrap();
    let ident = TableIdent::new(namespace, "removed".to_string());
    let paused = Arc::new(PausedLoadCatalog {
        inner: catalog.clone(),
        pause_next: AtomicBool::new(false),
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let provider = Arc::new(CachingTableProvider::new(
        paused.clone(),
        ident.clone(),
        delegate,
        None,
        None,
        Some(Arc::new(TableFreshness::new())),
        None,
    ));
    provider.refresh().await.unwrap();
    paused.pause_next.store(true, Ordering::Release);
    let old_load = tokio::spawn({
        let provider = provider.clone();
        async move { provider.load_current(LoadPath::Scan).await }
    });
    tokio::time::timeout(Duration::from_secs(2), paused.started.notified())
        .await
        .unwrap();
    catalog.drop_table(&ident).await.unwrap();
    assert!(provider.refresh().await.is_err());
    paused.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), old_load)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_err(),
        "old metadata must not override the observed DROP"
    );
    assert!(provider.cached.read().unwrap().is_none());
    assert!(provider.plan_cache_version().is_none());
    assert!(provider.current_provider().await.is_err());
}

#[tokio::test]
async fn foreign_same_name_recreation_requires_a_new_schema_provider() {
    let (catalog, context, namespace) = two_table_context().await;
    let ident = TableIdent::new(namespace.clone(), "removed".to_string());
    let original = catalog.load_table(&ident).await.unwrap();
    let schema = context
        .catalog(crate::context::CATALOG_NAME)
        .unwrap()
        .schema("drop_regression")
        .unwrap();
    catalog.drop_table(&ident).await.unwrap();
    assert!(schema.table("removed$snapshots").await.unwrap().is_none());
    let recreated = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("removed".to_string())
                .schema(original.metadata().current_schema().as_ref().clone())
                .build(),
        )
        .await
        .unwrap();
    assert_ne!(recreated.metadata().uuid(), original.metadata().uuid());
    assert!(schema.table("removed").await.unwrap().is_none());
    let restarted = crate::context::build_session_context_with(catalog, Some(1), None, None, 0)
        .await
        .unwrap();
    restarted
        .sql("SELECT * FROM drop_regression.removed")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

#[derive(Debug)]
struct PausedSchemaLookup {
    inner: datafusion::catalog::MemorySchemaProvider,
    pause_next: AtomicBool,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl SchemaProvider for PausedSchemaLookup {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }
    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let result = if name.ends_with("$snapshots") {
            Err(to_datafusion_error(iceberg::Error::new(
                ErrorKind::TableNotFound,
                "captured old404",
            )))
        } else {
            self.inner.table(name).await
        };
        if self.pause_next.swap(false, Ordering::AcqRel) {
            self.started.notify_one();
            self.release.notified().await;
        }
        result
    }
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }
    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }
}

fn lookup_delegate(field: &str) -> Arc<dyn TableProvider> {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    Arc::new(
        MemTable::try_new(
            Arc::new(Schema::new(vec![Field::new(field, DataType::Int64, true)])),
            vec![vec![]],
        )
        .unwrap(),
    )
}

async fn paused_schema_fixture() -> (Arc<PausedSchemaLookup>, Arc<CachingSchemaProvider>) {
    let (catalog, _, namespace) = two_table_context().await;
    let inner = Arc::new(PausedSchemaLookup {
        inner: datafusion::catalog::MemorySchemaProvider::new(),
        pause_next: AtomicBool::new(false),
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    inner
        .register_table("removed".to_string(), lookup_delegate("old_id"))
        .unwrap();
    let caching = Arc::new(
        CachingSchemaProvider::try_new(inner.clone(), catalog, namespace, None, None, false, None)
            .await
            .unwrap(),
    );
    (inner, caching)
}

#[tokio::test]
async fn registration_generation_fences_delayed_metadata_absence() {
    let (inner, caching) = paused_schema_fixture().await;
    inner.pause_next.store(true, Ordering::Release);
    let pending = tokio::spawn({
        let caching = caching.clone();
        async move { caching.table("removed$snapshots").await }
    });
    tokio::time::timeout(Duration::from_secs(2), inner.started.notified())
        .await
        .unwrap();
    caching.deregister_table("removed").unwrap();
    caching
        .register_table("removed".to_string(), lookup_delegate("new_id"))
        .unwrap();
    let replacement = caching.table("removed").await.unwrap().unwrap();
    inner.release.notify_one();
    assert!(pending.await.unwrap().unwrap().is_none());
    assert!(!caching.was_dropped("removed"));
    assert!(Arc::ptr_eq(
        &replacement,
        &caching.table("removed").await.unwrap().unwrap()
    ));
    assert!(!replacement
        .as_any()
        .downcast_ref::<CachingTableProvider>()
        .unwrap()
        .missing
        .load(Ordering::Acquire));
}

#[tokio::test]
async fn registration_generation_fences_delayed_plain_provider() {
    let (inner, caching) = paused_schema_fixture().await;
    caching.cached.write().unwrap().clear();
    inner.pause_next.store(true, Ordering::Release);
    let pending = tokio::spawn({
        let caching = caching.clone();
        async move { caching.table("removed").await }
    });
    tokio::time::timeout(Duration::from_secs(2), inner.started.notified())
        .await
        .unwrap();
    caching.deregister_table("removed").unwrap();
    caching
        .register_table("removed".to_string(), lookup_delegate("new_id"))
        .unwrap();
    let replacement = caching.table("removed").await.unwrap().unwrap();
    inner.release.notify_one();
    let error = pending
        .await
        .unwrap()
        .expect_err("stale plain provider must require retry");
    assert!(error.to_string().contains("registration changed"));
    assert_eq!(replacement.schema().field(0).name(), "new_id");
    assert!(Arc::ptr_eq(
        &replacement,
        &caching.table("removed").await.unwrap().unwrap()
    ));
}

async fn paused_time_travel_fixture() -> (
    Arc<PausedLoadCatalog>,
    Arc<CachingSchemaProvider>,
    TableIdent,
) {
    let (catalog, _, namespace) = two_table_context().await;
    let ident = TableIdent::new(namespace.clone(), "removed".to_string());
    let paused = Arc::new(PausedLoadCatalog {
        inner: catalog,
        pause_next: AtomicBool::new(false),
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let inner = Arc::new(datafusion::catalog::MemorySchemaProvider::new());
    inner
        .register_table("removed".to_string(), lookup_delegate("old_id"))
        .unwrap();
    let caching = Arc::new(
        CachingSchemaProvider::try_new(inner, paused.clone(), namespace, None, None, false, None)
            .await
            .unwrap(),
    );
    (paused, caching, ident)
}

#[tokio::test]
async fn registration_generation_fences_delayed_time_travel_absence() {
    let (paused, caching, ident) = paused_time_travel_fixture().await;
    paused.inner.drop_table(&ident).await.unwrap();
    paused.pause_next.store(true, Ordering::Release);
    let pending = tokio::spawn({
        let caching = caching.clone();
        async move { caching.table("removed@123").await }
    });
    tokio::time::timeout(Duration::from_secs(2), paused.started.notified())
        .await
        .unwrap();
    caching.deregister_table("removed").unwrap();
    caching
        .register_table("removed".to_string(), lookup_delegate("new_id"))
        .unwrap();
    let replacement = caching.table("removed").await.unwrap().unwrap();
    paused.release.notify_one();
    assert!(pending.await.unwrap().unwrap().is_none());
    assert!(!caching.was_dropped("removed"));
    assert!(Arc::ptr_eq(
        &replacement,
        &caching.table("removed").await.unwrap().unwrap()
    ));
}

#[tokio::test]
async fn registration_generation_fences_delayed_time_travel_provider() {
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let (paused, caching, ident) = paused_time_travel_fixture().await;
    let table = paused.inner.load_table(&ident).await.unwrap();
    // Only provider construction is exercised here, so the metadata-only
    // fixture never scans the referenced empty data file.
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path("memory://warehouse/unused-empty.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(0)
        .record_count(0)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .partition(Struct::empty())
        .build()
        .unwrap();
    let transaction = Transaction::new(&table);
    let table = transaction
        .fast_append()
        .add_data_files([file])
        .apply(transaction)
        .unwrap()
        .commit(paused.inner.as_ref())
        .await
        .unwrap();
    let reference = format!(
        "removed@{}",
        table.metadata().current_snapshot_id().unwrap()
    );
    paused.pause_next.store(true, Ordering::Release);
    let pending = tokio::spawn({
        let caching = caching.clone();
        let reference = reference.clone();
        async move { caching.table(&reference).await }
    });
    tokio::time::timeout(Duration::from_secs(2), paused.started.notified())
        .await
        .unwrap();
    caching.deregister_table("removed").unwrap();
    caching
        .register_table("removed".to_string(), lookup_delegate("new_id"))
        .unwrap();
    paused.release.notify_one();
    assert!(pending.await.unwrap().unwrap().is_some());
    assert!(
        caching.pinned.get(&reference).is_none(),
        "old snapshot must stay request-local"
    );
}

#[derive(Debug)]
struct PausedInsertDelegate {
    calls: AtomicU64,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl TableProvider for PausedInsertDelegate {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> ArrowSchemaRef {
        Arc::new(datafusion::arrow::datatypes::Schema::empty())
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "insert-only fixture".to_string(),
        ))
    }
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _operation: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.started.notify_one();
        self.release.notified().await;
        Ok(input)
    }
}

#[tokio::test]
async fn missing_provider_rejects_insert_before_and_after_delegate_planning() {
    let (catalog, _, namespace) = two_table_context().await;
    let delegate = Arc::new(PausedInsertDelegate {
        calls: AtomicU64::new(0),
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let provider = Arc::new(CachingTableProvider::new(
        catalog,
        TableIdent::new(namespace, "removed".to_string()),
        delegate.clone(),
        None,
        None,
        None,
        None,
    ));
    let input: Arc<dyn ExecutionPlan> = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
        delegate.schema(),
    ));
    let pending = tokio::spawn({
        let provider = provider.clone();
        let input = input.clone();
        async move {
            provider
                .insert_into(&SessionContext::new().state(), input, InsertOp::Append)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), delegate.started.notified())
        .await
        .unwrap();
    provider.mark_missing();
    delegate.release.notify_one();
    assert!(
        pending.await.unwrap().is_err(),
        "invalidated insert plan must not escape"
    );
    assert_eq!(delegate.calls.load(Ordering::Acquire), 1);
    assert!(provider
        .insert_into(&SessionContext::new().state(), input, InsertOp::Append)
        .await
        .is_err());
    assert_eq!(
        delegate.calls.load(Ordering::Acquire),
        1,
        "known-missing provider must not call its delegate"
    );
}
