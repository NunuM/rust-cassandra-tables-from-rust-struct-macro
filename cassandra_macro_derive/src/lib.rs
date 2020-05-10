//! Derive of the crate `cassandra_macro`
//!
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
#![recursion_limit = "128"]
extern crate proc_macro;

use proc_macro::TokenStream;
use std::collections::{BTreeMap, HashMap};

use syn;
use syn::NestedMeta;
use std::str::FromStr;

use quote::{quote, ToTokens};

#[proc_macro_derive(CassandraTable, attributes(column, table))]
pub fn cassandra_macro_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_cassandra_macro(&ast)
}

fn impl_cassandra_macro(ast: &syn::DeriveInput) -> TokenStream {
    let table_name = pascal_case_to_snake_case(&ast.ident.to_string());

    let mut table_meta = TableMeta::with_name(&table_name);

    // Ensure the macro is on a struct with named fields
    let fields = match ast.data {
        syn::Data::Struct(syn::DataStruct { ref fields, .. }) => {
            if fields.iter().any(|field| field.ident.is_none()) {
                panic!("struct has unnamed fields");
            }
            fields.iter().cloned().collect()
        }
        _ => panic!("#[derive(CassandraConfig)] can only be used with structs"),
    };

    extract_struct_attributes(&mut table_meta, &fields);

    for attr in ast.attrs.iter() {
        match attr.parse_meta() {
            Ok(syn::Meta::List(syn::MetaList { ref path, ref nested, .. })) => {
                let ident = path.get_ident().unwrap();
                match ident.to_string().as_ref() {
                    "table" => {
                        let mut meta_items_iter = nested.iter();

                        let mut meta_items = Vec::new();

                        while let Some(n) = meta_items_iter.next() {
                            meta_items.push(n);
                        }

                        let (key_space, options) = extract_table_properties(&meta_items);

                        table_meta.set_key_space(&key_space);
                        table_meta.set_table_options(&options);
                    }
                    _ => {}
                }
            }
            Err(_) => unreachable!(
                "Got something other than a list of attributes while checking table attribute"),
            _ => {}
        }
    }

    let create_table_sql = table_meta.create_table_cql();
    let drop_table_sql = table_meta.drop_table_cql();
    let key_space = table_meta.key_space();
    let table_name = table_meta.table_name();
    let select_by_key = table_meta.select_by_key();
    let select_by_keys = table_meta.select_by_keys();

    let update_by_key = table_meta.update_by_key();
    let update_by_keys = table_meta.update_by_keys();

    let delete_by_key = table_meta.delete_by_key();
    let delete_by_keys = table_meta.delete_by_keys();

    let store_stmt = table_meta.store_stmt();
    let store_values = table_meta.store_values();

    let (update_stmt, update_values) = table_meta.update_stmt()
        .unwrap_or((String::new(), proc_macro2::TokenStream::new()));

    let (delete_stmt, delete_values) = table_meta.delete_stmt();

    let ident = &ast.ident;

    // Helper is provided for handling complex generic types correctly and effortlessly
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    let impl_ast = quote!(

        impl #impl_generics CassandraTable for #ident #ty_generics #where_clause {

            fn create_table_cql() -> &'static str {
                &#create_table_sql
            }

            fn drop_table_cql() -> &'static str {
                &#drop_table_sql
            }

            fn key_space() -> &'static str {
                &#key_space
            }

            fn table_name() -> &'static str {
                &#table_name
            }

            fn select_by_primary_keys(projection: cassandra_macro::Projection) -> String {
                match projection {
                    Projection::Count => {
                         #select_by_key.to_string().replace("*", "count(*) as count")
                    },
                    Projection::All => {
                        #select_by_key.to_string()
                    },
                    Projection::Columns(c) => {
                         let column_selection : String = c.join(",");

                         #select_by_key.to_string().replace("*", column_selection.as_str())
                    }
                }
            }

            fn select_by_primary_and_cluster_keys(projection: cassandra_macro::Projection) -> String {
                match projection {
                    Projection::Count => {
                         #select_by_keys.to_string().replace("*", "count(*) as count")
                    },
                    Projection::All => {
                        #select_by_keys.to_string()
                    },
                    Projection::Columns(c) => {
                         let column_selection : String = c.join(",");

                         #select_by_keys.to_string().replace("*", column_selection.as_str())
                    }
                }
            }

            fn update_by_primary_keys(columns: Vec<String>) -> String {

                let update_columns = columns.iter().map(|c| format!(" {}=?", c)).collect::<Vec<String>>().join(",");

                #update_by_key.to_string().replace(":columns", update_columns.as_str())
            }

            fn update_by_primary_and_cluster_keys(columns: Vec<String>) -> String {

                let update_columns = columns.iter().map(|c| format!(" {}=?", c)).collect::<Vec<String>>().join(",");

                #update_by_keys.to_string().replace(":columns", update_columns.as_str())
            }

            fn delete_by_primary_keys() -> String {
                #delete_by_key.to_string()
            }

            fn delete_by_primary_and_cluster_keys() -> String {
                #delete_by_keys.to_string()
            }

            fn store_query(&self) -> cassandra_macro::StoreQuery {
                cassandra_macro::StoreQuery::new(#store_stmt.to_string(), query_values!(#store_values))
            }

            fn update_query(&self) -> Result<cassandra_macro::UpdateQuery, cassandra_macro::TableWithNoUpdatableColumnsError>
            {
               if #update_stmt.to_string().is_empty() {
                    return Err(cassandra_macro::TableWithNoUpdatableColumnsError::new(format!("Table {} does not have any updatable column", #table_name)) );
               }

               Ok(cassandra_macro::UpdateQuery::new(#update_stmt.to_string(), query_values!(#update_values)))
            }

            fn delete_query(&self) -> cassandra_macro::DeleteQuery {
                cassandra_macro::DeleteQuery::new(#delete_stmt.to_string(), query_values!(#delete_values))
            }

        }
    );

    impl_ast.into()
}

struct TableMeta {
    name: String,
    key_space: String,
    table_options: String,
    columns: HashMap<String, String>,
    static_columns: Vec<String>,
    primary_keys: BTreeMap<u8, String>,
    cluster_keys: BTreeMap<u8, (String, String)>,
}

/// @TODO Refactor duplicated code
impl TableMeta {
    fn with_name(name: &String) -> Self {
        TableMeta {
            name: name.to_owned(),
            key_space: String::new(),
            table_options: String::new(),
            columns: HashMap::new(),
            static_columns: Vec::new(),
            primary_keys: BTreeMap::new(),
            cluster_keys: BTreeMap::new(),
        }
    }

    fn delete_stmt(&self) -> (String, proc_macro2::TokenStream) {
        let pk_values: Vec<String> = self.primary_keys.values().map(|p| p.to_owned()).collect();

        let ck_values: Vec<String> = self.cluster_keys.values().map(|(c, _)| c.to_owned()).collect();

        let keys: Vec<(String, String)> = [&pk_values[..], &ck_values[..]]
            .concat()
            .iter()
            .map(|c| {
                (format!("{}=?", c), format!("self.{}.clone()", c))
            })
            .collect::<Vec<(String, String)>>();

        (format!("DELETE FROM {}.{} WHERE {}",
                 self.key_space,
                 self.name,
                 keys.iter().map(|(v, _)| v.to_owned())
                     .collect::<Vec<String>>()
                     .join(" AND ")),
         proc_macro2::TokenStream::from_str(keys.iter().map(|(_, v)| v.to_owned())
             .collect::<Vec<String>>()
             .join(",").as_str()).unwrap()
        )
    }

    fn update_stmt(&self) -> Option<(String, proc_macro2::TokenStream)> {
        let mut updatable_columns = Vec::new();

        for (column_name, _) in self.columns.iter() {
            if self.primary_keys.values().any(|p| p.eq(column_name)) {
                continue;
            }

            if self.cluster_keys.values().any(|ck| ck.0.eq(column_name)) {
                continue;
            }

            updatable_columns.push(column_name);
        }

        if updatable_columns.is_empty() {
            return None;
        }

        let update_values = updatable_columns.iter().map(|c| {
            (format!("{}=?", c), format!("self.{}.clone()", c))
        }).collect::<Vec<(String, String)>>();

        let p_keys = self.primary_keys.iter().map(|(_, pk)| {
            (format!("{}=?", pk), format!("self.{}.clone()", pk))
        }).collect::<Vec<(String, String)>>();

        let ck_keys = self.cluster_keys.iter().map(|(_, (ck, _))| {
            (format!("{}=?", ck), format!("self.{}.clone()", ck))
        }).collect::<Vec<(String, String)>>();

        let values: String = [&update_values[..], &p_keys[..], &ck_keys[..]]
            .concat()
            .iter()
            .map(|(_, c)| c.to_owned())
            .collect::<Vec<String>>()
            .join(",");

        let pk_values: Vec<String> = self.primary_keys.values().map(|p| p.to_owned()).collect();

        let ck_values: Vec<String> = self.cluster_keys.values().map(|(c, _)| c.to_owned()).collect();

        let keys: Vec<(String, String)> = [&pk_values[..], &ck_values[..]]
            .concat()
            .iter()
            .map(|c| {
                (format!("{}=?", c), format!("self.{}.clone()", c))
            })
            .collect::<Vec<(String, String)>>();

        Some((format!("UPDATE {}.{} SET {} WHERE {}",
                      self.key_space,
                      self.name,
                      update_values.iter().map(|(v, _)| v.to_owned()).collect::<Vec<String>>().join(","),
                      keys.iter().map(|(v, _)| v.to_owned()).collect::<Vec<String>>().join(" AND ")),
              proc_macro2::TokenStream::from_str(values.as_str()).unwrap()
        ))
    }

    fn store_stmt(&self) -> String {
        let fields = self.columns.iter().map(|(n, _)| n.to_owned()).collect::<Vec<String>>().join(",");

        let mut bind_marks = "?,".repeat(self.columns.len());
        bind_marks.pop();

        format!("INSERT INTO {}.{} ({}) VALUES ({})", self.key_space, self.name, fields, bind_marks)
    }

    fn store_values(&self) -> proc_macro2::TokenStream {
        let fields_tokens = self.columns.iter().map(|(v, _)| {
            format!("self.{}.clone()", v.to_owned())
        }).collect::<Vec<String>>().join(",");

        proc_macro2::TokenStream::from_str(fields_tokens.as_str()).unwrap()
    }

    fn set_key_space(&mut self, key_space: &String) {
        self.key_space = key_space.to_owned();
    }

    fn select_by_key(&self) -> String {
        let where_part = self.primary_keys
            .iter()
            .map(|(_, v)| format!(" {}=? ", v))
            .collect::<Vec<String>>()
            .join("AND");

        format!("SELECT * FROM {}.{} WHERE {}", self.key_space, self.name, where_part)
    }

    fn select_by_keys(&self) -> String {
        let pk_select = self.select_by_key();

        if self.cluster_keys.is_empty() {
            pk_select
        } else {
            let where_part = self.cluster_keys
                .iter()
                .map(|(_, (c, _))| format!(" {}=? ", c))
                .collect::<Vec<String>>()
                .join("AND");

            format!("{} AND {}", pk_select, where_part)
        }
    }

    fn update_by_key(&self) -> String {
        let where_part = self.primary_keys
            .iter()
            .map(|(_, v)| format!(" {}=? ", v))
            .collect::<Vec<String>>()
            .join("AND");

        format!("UPDATE {}.{} SET :columns WHERE {}", self.key_space, self.name, where_part)
    }

    fn update_by_keys(&self) -> String {
        let pk_select = self.update_by_key();

        if self.cluster_keys.is_empty() {
            pk_select
        } else {
            let where_part = self.cluster_keys
                .iter()
                .map(|(_, (c, _))| format!(" {}=? ", c))
                .collect::<Vec<String>>()
                .join("AND");

            format!("{} AND {}", pk_select, where_part)
        }
    }

    fn delete_by_key(&self) -> String {
        let where_part = self.primary_keys
            .iter()
            .map(|(_, v)| format!(" {}=? ", v))
            .collect::<Vec<String>>()
            .join("AND");

        format!("DELETE FROM {}.{} WHERE {}", self.key_space, self.name, where_part)
    }

    fn delete_by_keys(&self) -> String {
        let pk_select = self.delete_by_key();

        if self.cluster_keys.is_empty() {
            pk_select
        } else {
            let where_part = self.cluster_keys
                .iter()
                .map(|(_, (c, _))| format!(" {}=? ", c))
                .collect::<Vec<String>>()
                .join("AND");

            format!("{} AND {}", pk_select, where_part)
        }
    }

    fn set_table_options(&mut self, table_options: &String) {
        self.table_options = table_options.to_owned();
    }

    fn new_column(&mut self, name: &String, data_type: &String) {
        self.columns.insert(name.to_owned(), data_type.to_owned());
    }

    fn set_column_as_static(&mut self, name: &String) {
        self.static_columns.push(name.to_owned());
    }

    fn new_primary_key(&mut self, key: &String, position: Option<u8>) {
        self.primary_keys.insert(position.unwrap_or(1), key.to_owned());
    }

    fn new_cluster_key(&mut self, name: &String, order: &String, position: Option<u8>) {
        self.cluster_keys.insert(position.unwrap_or(1), (name.to_owned(), order.to_owned()));
    }

    fn key_space(&self) -> &String {
        &self.key_space
    }

    fn table_name(&self) -> &String {
        &self.name
    }

    fn drop_table_cql(&self) -> String {
        format!("DROP TABLE IF EXISTS {}.{}", self.key_space, self.name)
    }

    fn create_table_cql(&self) -> String {
        let mut table_options = String::new();
        let mut c_order = Vec::new();
        let mut c_keys = Vec::new();

        let columns: String = self.columns
            .iter()
            .map(|(k, t)| {
                if self.static_columns.contains(k) {
                    format!("{} {} STATIC", k, t)
                } else {
                    format!("{} {}", k, t.to_uppercase())
                }
            })
            .collect::<Vec<String>>()
            .join(",");

        let opt_parts: Vec<&str> = self.table_options.split("|").filter(|opt| !opt.is_empty()).collect();

        if self.cluster_keys.len() > 0 {
            for (_, (column, order)) in self.cluster_keys.iter() {
                c_order.push(format!("{} {}", column, order));
                c_keys.push(format!("{}", column))
            }
            table_options = format!("WITH CLUSTERING ORDER BY ({})", c_order.join(","));

            if opt_parts.len() > 0 {
                table_options = format!("{} AND {}", table_options, opt_parts.join(" AND "))
            }
        } else {
            if opt_parts.len() > 0 {
                table_options = format!("AND {}", opt_parts.join(" AND "))
            }
        }

        let primary_keys: String = self.primary_keys
            .iter()
            .map(|(_, k)| format!("{}", k))
            .collect::<Vec<String>>()
            .join(",");

        let create_stmt = format!("CREATE TABLE IF NOT EXISTS {}.{} ", self.key_space, self.name);

        if c_keys.len() > 0 {
            format!("{} ({}, PRIMARY KEY (({}), {}) ) {}", create_stmt, columns, primary_keys, c_keys.join(","), table_options)
        } else {
            format!("{} ({}, PRIMARY KEY ({}) ) {}", create_stmt, columns, primary_keys, table_options)
        }
    }
}

/// Parse struct attributes
fn extract_struct_attributes(table_meta: &mut TableMeta, fields: &Vec<syn::Field>) {
    for field in fields {
        let field_ident = field.ident.clone().unwrap().to_string();

        if field.attrs.len() > 0 {
            for attr in &field.attrs {
                if !attr.path.to_token_stream().to_string().contains("column") {
                    continue;
                }

                match attr.parse_meta() {
                    Ok(syn::Meta::List(syn::MetaList { ref nested, .. })) => {
                        let mut meta_items_iter = nested.iter();

                        let mut meta_items = Vec::new();

                        while let Some(n) = meta_items_iter.next() {
                            meta_items.push(n);
                        }

                        // only validation from there on
                        for meta_item in meta_items {
                            match *meta_item {
                                syn::NestedMeta::Meta(ref item) => match *item {
                                    syn::Meta::Path(ref name) => {
                                        match name.get_ident().unwrap().to_string().as_ref() {
                                            "primary_key" => {
                                                table_meta.new_primary_key(&field_ident, None);
                                            }
                                            "static" => {
                                                table_meta.set_column_as_static(&field_ident);
                                            }
                                            _ => panic!("Unexpected validator: {:?}", name.get_ident()),
                                        }
                                    }
                                    syn::Meta::NameValue(syn::MetaNameValue { ref path, ref lit, .. }) => {
                                        let ident = path.get_ident().unwrap();
                                        match ident.to_string().as_ref() {
                                            "type" => {
                                                table_meta.new_column(&field_ident.clone(), &lit_to_string(lit).unwrap_or(String::new()));
                                            }
                                            v => panic!("unexpected name value validator: {:?}", v),
                                        };
                                    }
                                    syn::Meta::List(syn::MetaList { ref path, ref nested, .. }) => {
                                        let mut meta_items_iter = nested.iter();

                                        let mut meta_items: Vec<&NestedMeta> = Vec::new();

                                        while let Some(n) = meta_items_iter.next() {
                                            meta_items.push(n);
                                        }

                                        let ident = path.get_ident().unwrap();
                                        match ident.to_string().as_ref() {
                                            "cluster_key" => {
                                                let (order, position) = extract_cluster_properties(&meta_items);

                                                table_meta.new_cluster_key(&field_ident, &order, Some(position));
                                            }
                                            "compound_key" => {
                                                let (_, position) = extract_cluster_properties(&meta_items);

                                                table_meta.new_primary_key(&field_ident, Some(position))
                                            }
                                            v => panic!("unexpected list validator: {:?}", v),
                                        }
                                    }
                                },
                                _ => unreachable!("Found a non Meta while looking for validators"),
                            };
                        }
                    }
                    Ok(syn::Meta::NameValue(_)) => panic!("Unexpected name=value argument"),
                    Err(e) => unreachable!(
                        "Got something other than a list of attributes while checking field `{}`: {:?}",
                        field_ident, e
                    ),
                    _ => {}
                }
            }
        }
    }
}

fn lit_to_string(lit: &syn::Lit) -> Option<String> {
    match *lit {
        syn::Lit::Str(ref s) => Some(s.value()),
        _ => None,
    }
}

fn lit_to_int(lit: &syn::Lit) -> Option<i64> {
    match *lit {
        syn::Lit::Int(ref s) => Some(s.base10_parse().unwrap()),
        _ => None,
    }
}

fn extract_cluster_properties(meta_items: &Vec<&syn::NestedMeta>) -> (String, u8) {
    let mut order = String::from("DESC");
    let mut position = 1;

    for meta_item in meta_items {
        if let syn::NestedMeta::Meta(ref item) = **meta_item {
            if let syn::Meta::NameValue(syn::MetaNameValue { ref path, ref lit, .. }) = *item {
                let ident = path.get_ident().unwrap();
                match ident.to_string().as_ref() {
                    "order" => {
                        order = lit_to_string(lit).unwrap_or(String::from("DESC"))
                    }
                    "position" => {
                        position = lit_to_int(lit).unwrap_or(1) as u8;
                    }
                    v => panic!("unknown argument `{}` for column `cluster_key`", v)
                }
            } else {
                panic!("unexpected item while parsing `cluster_key` column of field")
            }
        }
    }

    (order, position)
}

fn extract_table_properties(meta_items: &Vec<&syn::NestedMeta>) -> (String, String) {
    let mut keyspace = String::new();
    let mut options = String::new();

    for meta_item in meta_items {
        if let syn::NestedMeta::Meta(ref item) = **meta_item {
            if let syn::Meta::NameValue(syn::MetaNameValue { ref path, ref lit, .. }) = *item {
                let ident = path.get_ident().unwrap();
                match ident.to_string().as_ref() {
                    "keyspace" => {
                        keyspace = lit_to_string(lit).unwrap_or(String::new())
                    }
                    "options" => {
                        options = lit_to_string(lit).unwrap_or(String::new());
                    }
                    v => panic!("unknown argument `{}` for column `table`", v)
                }
            } else {
                panic!("unexpected item while parsing `table` column of field")
            }
        }
    }

    (keyspace, options)
}

const OFFSET: u8 = 32;
const UNDERSCORE: u8 = 95;

fn pascal_case_to_snake_case(table_name: &String) -> String {
    let word_size = table_name.len();

    if word_size < 2 {
        return String::from(table_name);
    }

    let mut counter = 1;
    let chars = table_name.as_bytes();
    let mut sk_table_name: Vec<u8> = Vec::new();

    if chars[0] < 90 {
        sk_table_name.push(chars[0] + OFFSET);
    } else {
        sk_table_name.push(chars[0]);
    }

    while counter < word_size {
        let current = chars[counter];

        if current < 90 {
            sk_table_name.push(UNDERSCORE);
            sk_table_name.push(current + OFFSET)
        } else {
            sk_table_name.push(current);
        }

        counter += 1;
    }

    String::from_utf8(sk_table_name).unwrap()
}

#[cfg(test)]
mod tests {
    use crate::pascal_case_to_snake_case;

    #[test]
    fn test_pascal_case_to_snake_case() {
        let table_1 = String::from("Test");

        let new_table_1 = pascal_case_to_snake_case(&table_1);

        assert_eq!(new_table_1, String::from("test"));

        let table_2 = String::from("TestHello");

        let new_table_2 = pascal_case_to_snake_case(&table_2);

        assert_eq!(new_table_2, String::from("test_hello"));
    }
}
