use datamodel::ast::{parser, SchemaAst};

pub type TestResult = Result<(), anyhow::Error>;

pub fn parse(datamodel_string: &str) -> SchemaAst {
    parser::parse_schema(datamodel_string).unwrap()
}

pub(crate) fn unique_migration_id() -> String {
    /// An atomic counter to generate unique migration IDs in tests.
    static MIGRATION_ID_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    format!(
        "migration-{}",
        MIGRATION_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}
