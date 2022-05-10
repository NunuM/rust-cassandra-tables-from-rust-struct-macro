#[macro_use]
extern crate cdrs;

use std::sync::Arc;

use cassandra_macro::{CassandraTable, DeleteQuery, Projection, UpdateQuery};
use cassandra_macro::StoreQuery;
use cassandra_macro_derive::CassandraTable;
use cdrs::authenticators::StaticPasswordAuthenticator;
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::cluster::session::{new_lz4, Session};
use cdrs::Error as CassandraDriverError;
use cdrs::frame::TryFromRow;
use cdrs::load_balancing::RoundRobinSync;
use cdrs::query::{QueryExecutor, QueryValues};
use cdrs::types::ByName;
use cdrs::types::rows::Row;
use cdrs::types::value::Value;
use chrono::Utc;
use uuid::Uuid;

#[table(keyspace = "test", options = "comment='Only for RUST users' | COMPACTION = {'class':'SizeTieredCompactionStrategy'}")]
#[derive(Debug, CassandraTable)]
pub struct UserTestExample {
    #[column(type = "TEXT", primary_key)]
    username: String,

    #[column(type = "UUID")]
    user_internal_id: Uuid,

    #[column(type = "TEXT")]
    first_name: String,

    #[column(type = "TIMESTAMP", cluster_key(order = "ASC", position = 1))]
    created: i64,

    #[column(type = "TIMESTAMP")]
    updated: i64,
}

impl UserTestExample {
    fn set_first_name(&mut self, first_name: String) {
        self.first_name = first_name;
    }
}

impl Default for UserTestExample {
    fn default() -> Self {
        UserTestExample {
            username: "Rust".to_string(),
            user_internal_id: Uuid::new_v4(),
            first_name: "rust".to_string(),
            created: Utc::now().timestamp_millis(),
            updated: Utc::now().timestamp_millis(),
        }
    }
}

impl TryFromRow for UserTestExample {
    fn try_from_row(row: Row) -> Result<Self, cdrs::Error> {
        let username = row.r_by_name::<String>("username")?;
        let user_internal_id = row.r_by_name::<Uuid>("user_internal_id")?;
        let first_name = row.r_by_name::<String>("first_name")?;
        let created: i64 = row.r_by_name::<i64>("created")?;
        let updated: i64 = row.r_by_name::<i64>("updated")?;

        Ok(UserTestExample {
            username,
            user_internal_id,
            first_name,
            created,
            updated,
        })
    }
}


pub struct CassandraConfig {
    nodes: Vec<String>,
    user: String,
    password: String,
}

pub struct CassandraDriver {
    connection: Arc<Session<RoundRobinSync<TcpConnectionPool<StaticPasswordAuthenticator>>>>
}

impl CassandraDriver {
    pub fn execute_simple_statement<Q: ToString>(&self, query: Q) -> Result<bool, CassandraDriverError> {
        match self.connection.query(query) {
            Ok(_) => Ok(true),
            Err(e) => {
                Err(e)
            }
        }
    }

    pub fn execute_store_query(&self, query: &StoreQuery) -> Result<bool, CassandraDriverError> {
        self.execute_query(query.query(), query.values())
    }

    pub fn execute_update_query(&self, query: &UpdateQuery) -> Result<bool, CassandraDriverError> {
        self.execute_query(query.query(), query.values())
    }

    pub fn execute_delete_query(&self, query: &DeleteQuery) -> Result<bool, CassandraDriverError> {
        self.execute_query(query.query(), query.values())
    }

    pub fn execute_query(&self, query: &String, values: &QueryValues) -> Result<bool, CassandraDriverError> {
        let result = self.connection
            .query_with_values(query, values.to_owned());

        result.map(|_| true)
    }

    pub fn find<T: TryFromRow + CassandraTable>(&self, keys: Vec<String>) -> Result<Option<T>, CassandraDriverError> {
        let stmt = T::select_by_primary_keys(Projection::All);

        let values = keys.iter().map(|k| Value::from(k.to_string())).collect::<Vec<Value>>();

        let result_frame = self.connection.query_with_values(stmt, QueryValues::SimpleValues(values))?;

        Ok(result_frame.get_body()?.into_rows()
            .map(|r| { r.first().map(|r| T::try_from_row(r.to_owned()).unwrap()) }).flatten())
    }

    pub fn new_from_config(cassandra_configs: &CassandraConfig) -> Self {
        let mut nodes = Vec::with_capacity(cassandra_configs.nodes.len());

        for node in cassandra_configs.nodes.iter() {
            let authenticator: StaticPasswordAuthenticator = StaticPasswordAuthenticator::new(cassandra_configs.user.clone(),
                                                                                              cassandra_configs.password.clone());

            let node_tcp = NodeTcpConfigBuilder::new(node.as_str(), authenticator).build();

            nodes.push(node_tcp);
        }

        let cluster_config = ClusterTcpConfig(nodes);

        let cassandra_session = new_lz4(&cluster_config, RoundRobinSync::new())
            .expect("Cassandra session must be created");

        CassandraDriver {
            connection: Arc::new(cassandra_session)
        }
    }
}

fn main() {
    let driver_conf = CassandraConfig {
        nodes: vec!["192.168.1.41:9042".to_string()],
        user: String::from("test"),
        password: String::from("test"),
    };

    let connection = CassandraDriver::new_from_config(&driver_conf);

    println!("Keyspace:.{}.", UserTestExample::key_space());
    println!("Table name:.{}.", UserTestExample::table_name());
    println!("Creating table:{}", UserTestExample::create_table_cql());
    connection.execute_simple_statement(UserTestExample::create_table_cql()).expect("Must create table");

    println!("You can test those by yourself");
    println!("{}", UserTestExample::select_by_primary_keys(Projection::Columns(vec!["created".to_string()])));
    println!("{}", UserTestExample::select_by_primary_and_cluster_keys(Projection::All));
    println!("{}", UserTestExample::update_by_primary_keys(vec!["updated".to_string()]));
    println!("{}", UserTestExample::update_by_primary_and_cluster_keys(vec!["updated".to_string()]));
    println!("{}", UserTestExample::delete_by_primary_keys());
    println!("{}", UserTestExample::delete_by_primary_and_cluster_keys());

    let mut rust_user = UserTestExample::default();

    println!("Storing rust: {}", rust_user.store_query().query());
    connection.execute_store_query(&rust_user.store_query()).expect("User must be stored");

    let rust_user_from_db: Option<UserTestExample> = connection.find::<UserTestExample>(vec!["Rust".to_string()]).unwrap();
    assert!(rust_user_from_db.unwrap().username.eq(&rust_user.username), "Must be the same");

    println!("Update rust:{}", rust_user.update_query().unwrap().query());
    rust_user.set_first_name(String::from("IamRoot"));

    connection.execute_update_query(&rust_user.update_query().unwrap()).unwrap();

    let rust_user_from_db_1 = connection.find::<UserTestExample>(vec!["Rust".to_string()]).unwrap();

    assert!(rust_user_from_db_1.unwrap().username.eq(&rust_user.username), "Must be the same");

    println!("Delete:{}", rust_user.delete_query().query());
    connection.execute_delete_query(&rust_user.delete_query()).expect("Must be deleted");

    println!("Dropping table: {}", UserTestExample::drop_table_cql());
    connection.execute_simple_statement(UserTestExample::drop_table_cql()).expect("Table must be removed");
}
