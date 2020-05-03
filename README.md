### Create Cassandra Migrations from Rust Structs

Custom derive to simplify cassandra table creation from rust structs

#### Installation

```
cassandra_macro = "0.10"
cassandra_macro_derive = "0.10"
```

#### Example with __primary key__

```rust
use cassandra_macro::Cassandra;
use cassandra_macro_derive::Cassandra;

#[table(keyspace = "fog")]
#[derive(Debug, Cassandra)]
pub struct TestRust {
    #[column(type = "TEXT", primary_key)]
    key_one: String,
}

fn main() {
    let f = TestRust {
        key_one: String::from("one")
    };

    println!("{}", f.create_table_cql()); /// CREATE TABLE IF NOT EXISTS fog.test_rust (key_one TEXT, PRIMARY KEY (key_one));
    println!("{}", f.drop_table_cql());   /// DROP TABLE IF EXISTS fog.test_rust
    println!("{}", f.key_space());        /// fog
    println!("{}", f.table_name());       /// test_rust
}

```

#### Example with __compound key__

```rust
use cassandra_macro::Cassandra;
use cassandra_macro_derive::Cassandra;

#[table(keyspace = "fog")]
#[derive(Debug, Cassandra)]
pub struct TestRust {
    #[column(type = "UUID", compound_key(position = 2))]
    key_one: String,

    #[column(type = "TEXT", compound_key(position = 1))]
    key_two: String,

}


fn main() {
    let f = TestRust {
        key_one: String::from("one"),
        key_two: String::from("two")
    };

    println!("{}", f.create_table_cql()); ///CREATE TABLE IF NOT EXISTS fog.test_rust (key_one UUID,key_two TEXT, PRIMARY KEY (key_two,key_one) ) ; 
    println!("{}", f.drop_table_cql());
    println!("{}", f.key_space());
    println!("{}", f.table_name());
}
```


#### Example with __compound key & cluster key & options__

```rust
use cassandra_macro::Cassandra;
use cassandra_macro_derive::Cassandra;

/// Options are separated with '|' char
#[table(keyspace = "fog", options = "comment='From RUST' | COMPACTION = { 'class' : 'SizeTieredCompactionStrategy' }")]
#[derive(Debug, Cassandra)]
pub struct TestRust {
    #[column(type = "UUID", compound_key(position = 2))]
    key_one: String,

    #[column(type = "TEXT", compound_key(position = 1))]
    key_two: String,

    #[column(type = "TIMESTAMP", cluster_key(order = "DESC", position = 1))]
    created: i64,

    updated: i64, // Field with no annotation is ignored
}


fn main() {
    let f = TestRust {
        key_one: String::from("one"),
        key_two: String::from("two"),
        created: 0,
        updated: 0,
    };

    println!("{}", f.create_table_cql()); /// CREATE TABLE IF NOT EXISTS fog.test_rust ( key_two TEXT,created TIMESTAMP,key_one UUID, PRIMARY KEY ((key_two,key_one), created) ) WITH CLUSTERING ORDER BY (created DESC) AND comment='From RUST'  AND  COMPACTION = { 'class' : 'SizeTieredCompactionStrategy' };
    println!("{}", f.drop_table_cql());
    println!("{}", f.key_space());
    println!("{}", f.table_name());
}

```


