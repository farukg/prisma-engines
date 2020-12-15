mod capabilities;
pub mod mssql;
mod tags;

pub use capabilities::*;
pub use tags::*;

use enumflags2::BitFlags;
use once_cell::sync::Lazy;

use crate::{
    mariadb_url, mssql_2017_url, mssql_2019_url, mysql_5_6_url, mysql_8_url, mysql_url, postgres_10_url,
    postgres_11_url, postgres_12_url, postgres_13_url, postgres_9_url, sqlite_test_url,
};

fn connector_names() -> Vec<(
    &'static str,
    BitFlags<Tags>,
    &'static (dyn Fn(&str) -> String + Send + Sync + 'static),
)> {
    vec![
        ("mssql_2017", Tags::Mssql2017 | Tags::Mssql, &mssql_2017_url),
        ("mssql_2019", Tags::Mssql2019 | Tags::Mssql, &mssql_2019_url),
        ("mysql_8", Tags::Mysql | Tags::Mysql8, &mysql_8_url),
        ("mysql", Tags::Mysql.into(), &mysql_url),
        ("mysql_5_6", Tags::Mysql | Tags::Mysql56, &mysql_5_6_url),
        ("postgres9", Tags::Postgres.into(), &postgres_9_url),
        ("postgres", Tags::Postgres.into(), &postgres_10_url),
        ("postgres11", Tags::Postgres.into(), &postgres_11_url),
        ("postgres12", Tags::Postgres.into(), &postgres_12_url),
        ("postgres13", Tags::Postgres.into(), &postgres_13_url),
        ("mysql_mariadb", Tags::Mysql | Tags::Mariadb, &mariadb_url),
        ("sqlite", Tags::Sqlite.into(), &sqlite_test_url),
    ]
}

fn postgres_capabilities() -> BitFlags<Capabilities> {
    Capabilities::ScalarLists | Capabilities::Enums | Capabilities::Json
}

fn mysql_capabilities() -> BitFlags<Capabilities> {
    Capabilities::Enums | Capabilities::Json
}

fn mysql_5_6_capabilities() -> BitFlags<Capabilities> {
    Capabilities::Enums.into()
}

fn mssql_2017_capabilities() -> BitFlags<Capabilities> {
    BitFlags::empty()
}

fn mssql_2019_capabilities() -> BitFlags<Capabilities> {
    BitFlags::empty()
}

fn infer_capabilities(tags: BitFlags<Tags>) -> BitFlags<Capabilities> {
    if tags.intersects(Tags::Postgres) {
        return postgres_capabilities();
    }

    if tags.intersects(Tags::Mysql56) {
        return mysql_5_6_capabilities();
    }

    if tags.intersects(Tags::Mysql) {
        return mysql_capabilities();
    }

    if tags.intersects(Tags::Mssql2017) {
        return mssql_2017_capabilities();
    }

    if tags.intersects(Tags::Mssql2019) {
        return mssql_2019_capabilities();
    }

    BitFlags::empty()
}

pub static CONNECTORS: Lazy<Connectors> = Lazy::new(|| {
    let connectors: Vec<Connector> = connector_names()
        .iter()
        .map(|(name, tags, url_fn)| Connector {
            name: (*name).to_owned(),
            test_api_factory_name: format!("{}_test_api", name),
            capabilities: infer_capabilities(*tags),
            tags: *tags,
            url_fn: *url_fn,
        })
        .collect();

    Connectors::new(connectors)
});

pub struct Connectors {
    connectors: Vec<Connector>,
}

impl Connectors {
    fn new(connectors: Vec<Connector>) -> Connectors {
        Connectors { connectors }
    }

    pub fn all(&self) -> impl Iterator<Item = &Connector> {
        self.connectors.iter()
    }

    pub fn len(&self) -> usize {
        self.connectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Represents a connector to be tested.
pub struct Connector {
    name: String,
    test_api_factory_name: String,
    pub capabilities: BitFlags<Capabilities>,
    pub tags: BitFlags<Tags>,
    url_fn: &'static (dyn Fn(&str) -> String + Send + Sync + 'static),
}

impl Connector {
    /// The name of the connector.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The name of the API factory function for that connector.
    pub fn test_api(&self) -> &str {
        &self.test_api_factory_name
    }

    pub fn build_test_connection_string(&self, db_name: &str) -> String {
        (*self.url_fn)(db_name)
    }
}
