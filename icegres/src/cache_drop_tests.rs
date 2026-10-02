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
        .err()
        .expect("the dropped table must not resolve");
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
            .err()
            .expect("catalog failures must remain visible");
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
        .err()
        .expect("known deletion must never serve the retained snapshot");
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
    caching.forget_dropped_table("removed");
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
    caching.forget_dropped_table("removed");
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
        let table = self.inner.load_table(ident).await?;
        if pause {
            self.started.notify_one();
            self.release.notified().await;
        }
        Ok(table)
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
