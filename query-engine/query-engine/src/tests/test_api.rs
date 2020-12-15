use crate::{
    context::PrismaContext,
    request_handlers::{graphql, GraphQlBody, SingleQuery},
    PrismaResponse,
};
use migration_core::{
    api::{GenericApi, MigrationApi},
    commands::SchemaPushInput,
};
use quaint::{
    ast::*,
    connector::ConnectionInfo,
    visitor::{self, Visitor},
};
use sql_migration_connector::{SqlMigration, SqlMigrationConnector};
use std::sync::Arc;
use test_setup::{
    connectors::Tags, mssql_2019_test_config, mysql_test_config, postgres_12_test_config, sqlite_test_config,
    TestAPIArgs,
};

pub struct QueryEngine {
    context: Arc<PrismaContext>,
}

impl QueryEngine {
    #[allow(dead_code)]
    pub fn new(ctx: PrismaContext) -> Self {
        QueryEngine { context: Arc::new(ctx) }
    }

    pub async fn request(&self, body: impl Into<SingleQuery>) -> serde_json::Value {
        let body = GraphQlBody::Single(body.into());
        let cx = self.context.clone();
        match graphql::handle(body, cx).await {
            PrismaResponse::Single(response) => serde_json::to_value(response).unwrap(),
            _ => unreachable!(),
        }
    }
}

pub struct TestApi {
    connection_info: ConnectionInfo,
    migration_api: MigrationApi<SqlMigrationConnector, SqlMigration>,
    config: String,
}

impl TestApi {
    pub async fn new(args: TestAPIArgs) -> TestApi {
        let db_name = args.test_function_name;
        let connection_info = ConnectionInfo::from_url(args.test_database_url).unwrap();

        let migration_api = MigrationApi::new(SqlMigrationConnector::new(args.test_database_url).await.unwrap())
            .await
            .unwrap();

        let config = if args.test_tag.contains(Tags::Mysql) {
            mysql_test_config(db_name)
        } else if args.test_tag.contains(Tags::Mssql) {
            mssql_2019_test_config(db_name)
        } else if args.test_tag.contains(Tags::Postgres) {
            postgres_12_test_config(db_name)
        } else if args.test_tag.contains(Tags::Sqlite) {
            sqlite_test_config(db_name)
        } else {
            unreachable!()
        };

        TestApi {
            connection_info,
            migration_api,
            config,
        }
    }

    pub async fn create_engine(&self, datamodel: &str) -> anyhow::Result<QueryEngine> {
        feature_flags::initialize(&[String::from("all")]).unwrap();

        let datamodel_string = format!("{}\n\n{}", self.config, datamodel);
        let dml = datamodel::parse_datamodel(&datamodel_string).unwrap().subject;
        let config = datamodel::parse_configuration(&datamodel_string).unwrap();

        self.migration_api
            .schema_push(&SchemaPushInput {
                schema: datamodel_string,
                force: true,
                assume_empty: true,
            })
            .await?;

        let context = PrismaContext::builder(config.subject, dml)
            .enable_raw_queries(true)
            .build()
            .await
            .unwrap();

        Ok(QueryEngine {
            context: Arc::new(context),
        })
    }

    pub fn connection_info(&self) -> &ConnectionInfo {
        &self.connection_info
    }

    pub fn to_sql_string<'a>(&'a self, query: impl Into<Query<'a>>) -> quaint::Result<(String, Vec<Value>)> {
        match self.connection_info() {
            ConnectionInfo::Postgres(..) => visitor::Postgres::build(query),
            ConnectionInfo::Mysql(..) => visitor::Mysql::build(query),
            ConnectionInfo::Sqlite { .. } | ConnectionInfo::InMemorySqlite { .. } => visitor::Sqlite::build(query),
            ConnectionInfo::Mssql(_) => visitor::Mssql::build(query),
        }
    }
}
