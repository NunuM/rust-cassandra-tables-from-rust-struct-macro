//! Create Cassandra tables and CRUD CQL prepared statements
//! from Rust structs.
//!
//! This crate intends to reduce the boilerplate when create
//! and executing basic Cassandra statements. We can see that
//! the Rust struct holds the metadata for executing basic
//! operations. Before this crate, you will need to write the
//! Cassandra Table definition, create the Rust struct and
//! manually write all CQL statements for all operations.
//!
//! Worse than duplicated metadata, is refactoring code when you
//! add a new table, or change tables names and the statements
//! blows up because the table no longer exists.
//!
//! This crate is not perfect, and you are welcome to contribute.
//!
//! # Installation
//!
//! The crate `cdrs` is required since we make prepared statements
//! and the values of your struct must be mapped into Cassandra
//! datatypes and this crate does this mapping, and also, you will
//! need a driver between Rust code and your Cassandra cluster.
//!
//! ```toml
//! [dependencies]
//! cdrs = { version = "2" }
//! cassandra_macro = "0.1.1"
//! cassandra_macro_derive = "0.1.1"
//! ```
//!
//! In your `main.rs`
//!
//! ```
//! #[macro_use]
//! extern crate cdrs;
//! ```
//!
//! # Example
//! ```
//! #[macro_use]
//!extern crate cdrs;
//!
//!use std::sync::Arc;
//!
//!use cassandra_macro::{CassandraTable, DeleteQuery, Projection, UpdateQuery};
//!use cassandra_macro::StoreQuery;
//!use cassandra_macro_derive::CassandraTable;
//!use cdrs::authenticators::StaticPasswordAuthenticator;
//!use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
//!use cdrs::cluster::session::{new_lz4, Session};
//!use cdrs::Error as CassandraDriverError;
//!use cdrs::frame::TryFromRow;
//!use cdrs::load_balancing::RoundRobinSync;
//!use cdrs::query::{QueryExecutor, QueryValues};
//!use cdrs::types::ByName;
//!use cdrs::types::rows::Row;
//!use cdrs::types::value::Value;
//!use chrono::Utc;
//!use uuid::Uuid;
//!
//!#[table(keyspace = "test", options = "comment='Only for RUST users' | COMPACTION = {'class':'SizeTieredCompactionStrategy'}")]
//!#[derive(Debug, CassandraTable)]
//!pub struct User {
//!    #[column(type = "TEXT", primary_key)]
//!    username: String,
//!
//!    #[column(type = "UUID")]
//!    user_internal_id: Uuid,
//!
//!    #[column(type = "TEXT")]
//!    first_name: String,
//!
//!    #[column(type = "TIMESTAMP", cluster_key(order = "ASC", position = 1))]
//!    created: i64,
//!
//!    #[column(type = "TIMESTAMP")]
//!    updated: i64,
//!}
//!
//!impl User {
//!    fn set_first_name(&mut self, first_name: String) {
//!        self.first_name = first_name;
//!    }
//!}
//!
//!impl Default for User {
//!    fn default() -> Self {
//!        User {
//!            username: "Rust".to_string(),
//!            user_internal_id: Uuid::new_v4(),
//!            first_name: "rust".to_string(),
//!            created: Utc::now().timestamp_millis(),
//!            updated: Utc::now().timestamp_millis(),
//!        }
//!    }
//!}
//!
//!impl TryFromRow for User {
//!    fn try_from_row(row: Row) -> Result<Self, cdrs::Error> {
//!        let username = row.r_by_name::<String>("username")?;
//!        let user_internal_id = row.r_by_name::<Uuid>("user_internal_id")?;
//!        let first_name = row.r_by_name::<String>("first_name")?;
//!        let created: i64 = row.r_by_name::<i64>("created")?;
//!        let updated: i64 = row.r_by_name::<i64>("updated")?;
//!
//!        Ok(User {
//!            username,
//!            user_internal_id,
//!            first_name,
//!            created,
//!            updated,
//!        })
//!    }
//!}
//!
//!
//!pub struct CassandraConfig {
//!    nodes: Vec<String>,
//!    user: String,
//!    password: String,
//!}
//!
//!pub struct CassandraDriver {
//!    connection: Arc<Session<RoundRobinSync<TcpConnectionPool<StaticPasswordAuthenticator>>>>
//!}
//!
//!impl CassandraDriver {
//!    pub fn execute_simple_statement<Q: ToString>(&self, query: Q) -> Result<bool, CassandraDriverError> {
//!        match self.connection.query(query) {
//!            Ok(_) => Ok(true),
//!            Err(e) => {
//!                Err(e)
//!            }
//!        }
//!    }
//!
//!    pub fn execute_store_query(&self, query: &StoreQuery) -> Result<bool, CassandraDriverError> {
//!        self.execute_query(query.query(), query.values())
//!    }
//!
//!    pub fn execute_update_query(&self, query: &UpdateQuery) -> Result<bool, CassandraDriverError> {
//!        self.execute_query(query.query(), query.values())
//!    }
//!
//!    pub fn execute_delete_query(&self, query: &DeleteQuery) -> Result<bool, CassandraDriverError> {
//!        self.execute_query(query.query(), query.values())
//!    }
//!
//!    pub fn execute_query(&self, query: &String, values: &QueryValues) -> Result<bool, CassandraDriverError> {
//!        let result = self.connection
//!            .query_with_values(query, values.to_owned());
//!
//!        result.map(|_| true)
//!    }
//!
//!    pub fn find<T: TryFromRow + CassandraTable>(&self, keys: Vec<String>) -> Result<Option<T>, CassandraDriverError> {
//!        let stmt = T::select_by_primary_keys(Projection::All);
//!
//!        let values = keys.iter().map(|k| Value::from(k.to_string())).collect::<Vec<Value>>();
//!
//!        let result_frame = self.connection.query_with_values(stmt, QueryValues::SimpleValues(values))?;
//!
//!        Ok(result_frame.get_body()?.into_rows()
//!            .map(|r| { r.first().map(|r| T::try_from_row(r.to_owned()).unwrap()) }).flatten())
//!    }
//!
//!    pub fn new_from_config(cassandra_configs: &CassandraConfig) -> Self {
//!        let mut nodes = Vec::with_capacity(cassandra_configs.nodes.len());
//!
//!        for node in cassandra_configs.nodes.iter() {
//!            let authenticator: StaticPasswordAuthenticator = StaticPasswordAuthenticator::new(cassandra_configs.user.clone(),
//!                                                                                              cassandra_configs.password.clone());
//!
//!            let node_tcp = NodeTcpConfigBuilder::new(node.as_str(), authenticator).build();
//!
//!            nodes.push(node_tcp);
//!        }
//!
//!        let cluster_config = ClusterTcpConfig(nodes);
//!
//!        let cassandra_session = new_lz4(&cluster_config, RoundRobinSync::new())
//!            .expect("Cassandra session must be created");
//!
//!        CassandraDriver {
//!            connection: Arc::new(cassandra_session)
//!        }
//!    }
//!}
//!
//!fn main() {
//!    let driver_conf = CassandraConfig {
//!        nodes: vec!["aella:9042".to_string()],
//!        user: String::from("mazikeen"),
//!        password: String::from("NunoTiago12_34"),
//!    };
//!
//!    let connection = CassandraDriver::new_from_config(&driver_conf);
//!
//!    println!("Keyspace:.{}.", User::key_space());
//!    println!("Table name:.{}.", User::table_name());
//!    println!("Creating table:{}", User::create_table_cql());
//!    connection.execute_simple_statement(User::create_table_cql()).expect("Must create table");
//!
//!    println!("You can test those by yourself");
//!    println!("{}", User::select_by_primary_keys(Projection::Columns(vec!["created".to_string()])));
//!    println!("{}", User::select_by_primary_and_cluster_keys(Projection::All));
//!    println!("{}", User::update_by_primary_keys(vec!["updated".to_string()]));
//!    println!("{}", User::update_by_primary_and_cluster_keys(vec!["updated".to_string()]));
//!    println!("{}", User::delete_by_primary_keys());
//!    println!("{}", User::delete_by_primary_and_cluster_keys());
//!
//!    let mut rust_user = User::default();
//!
//!    println!("Storing rust: {}", rust_user.store_query().query());
//!    connection.execute_store_query(&rust_user.store_query()).expect("User must be stored");
//!
//!    let rust_user_from_db: Option<User> = connection.find::<User>(vec!["Rust".to_string()]).unwrap();
//!    assert!(rust_user_from_db.unwrap().username.eq(&rust_user.username), "Must be the same");
//!
//!    println!("Update rust:{}", rust_user.update_query().unwrap().query());
//!    rust_user.set_first_name(String::from("IamRoot"));
//!
//!    connection.execute_update_query(&rust_user.update_query().unwrap()).unwrap();
//!
//!    let rust_user_from_db_1 = connection.find::<User>(vec!["Rust".to_string()]).unwrap();
//!
//!    assert!(rust_user_from_db_1.unwrap().username.eq(&rust_user.username), "Must be the same");
//!
//!    println!("Delete:{}", rust_user.delete_query().query());
//!    connection.execute_delete_query(&rust_user.delete_query()).expect("Must be deleted");
//!
//!    println!("Dropping table: {}", User::drop_table_cql());
//!    connection.execute_simple_statement(User::drop_table_cql()).expect("Table must be removed");
//!}
//! ```
use cdrs::query::QueryValues;
use std::fmt::{Display, Formatter};

pub enum Projection {
    Count,
    All,
    Columns(Vec<String>),
}

pub trait CassandraTable {
    /// key space
    fn key_space() -> &'static str;

    /// Table name
    fn table_name() -> &'static str;

    /// CQL for table creation
    fn create_table_cql() -> &'static str;

    /// CQL for drop table
    fn drop_table_cql() -> &'static str;

    /// Prepared statement for selection by primary keys
    fn select_by_primary_keys(projection: Projection) -> String;

    /// Prepared statement for selection by primary keys and cluster keys
    fn select_by_primary_and_cluster_keys(projection: Projection) -> String;

    /// Prepared statement for update by primary keys
    fn update_by_primary_keys(columns: Vec<String>) -> String;

    /// Prepared statement for update by primary keys and cluster keys
    fn update_by_primary_and_cluster_keys(columns: Vec<String>) -> String;

    /// Prepared statement for delete by primary keys
    fn delete_by_primary_keys() -> String;

    /// Prepared statement for delete by primary keys and cluster key
    fn delete_by_primary_and_cluster_keys() -> String;

    /// Create `StoreQuery` containing the prepared statement
    /// to store this entity
    fn store_query(&self) -> StoreQuery;

    /// Create `UpdateQuery` containing the prepared statement
    /// to update this entity
    ///
    /// The statement only can update columns that are not
    /// part of the primary keys.
    fn update_query(&self) -> Result<UpdateQuery, TableWithNoUpdatableColumnsError>;

    /// Create `DeleteQuery` containing the prepared statement
    /// to delete this entity
    fn delete_query(&self) -> DeleteQuery;
}

#[derive(Debug)]
pub struct StoreQuery {
    query: String,
    values: QueryValues,
}

impl StoreQuery {
    /// New instance
    pub fn new(query: String, values: QueryValues) -> Self {
        StoreQuery { query, values }
    }

    /// Prepared statement for insertion
    pub fn query(&self) -> &String {
        &self.query
    }

    /// Values for executing prepared statement
    pub fn values(&self) -> &QueryValues {
        &self.values
    }
}

impl Display for StoreQuery {
    /// Only display the prepared statement
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "query:{}", self.query)
    }
}

#[derive(Debug)]
pub struct UpdateQuery {
    query: String,
    values: QueryValues,
}

impl UpdateQuery {
    /// New instance
    pub fn new(query: String, values: QueryValues) -> Self {
        UpdateQuery { query, values }
    }
    /// Prepared statement for update
    pub fn query(&self) -> &String {
        &self.query
    }
    /// Values for executing prepared statement
    pub fn values(&self) -> &QueryValues {
        &self.values
    }
}

impl Display for UpdateQuery {
    /// Only display the prepared statement
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "query:{}", self.query)
    }
}

#[derive(Debug)]
pub struct DeleteQuery {
    query: String,
    values: QueryValues,
}

impl DeleteQuery {
    /// New instance
    pub fn new(query: String, values: QueryValues) -> Self {
        DeleteQuery { query, values }
    }

    /// Prepared statement for deletion
    pub fn query(&self) -> &String {
        &self.query
    }

    /// Values for executing prepared statement
    pub fn values(&self) -> &QueryValues {
        &self.values
    }
}

impl Display for DeleteQuery {
    /// Only display the prepared statement
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "query:{}", self.query)
    }
}

/// Error if user tries to create
/// invalid update statement
#[derive(Debug)]
pub struct TableWithNoUpdatableColumnsError {
    message: String
}

impl TableWithNoUpdatableColumnsError {
    pub fn new(message: String) -> Self {
        TableWithNoUpdatableColumnsError { message }
    }
}

impl std::error::Error for TableWithNoUpdatableColumnsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self)
    }
}

impl std::fmt::Display for TableWithNoUpdatableColumnsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message.as_str())
    }
}