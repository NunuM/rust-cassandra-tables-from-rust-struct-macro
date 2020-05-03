pub trait Cassandra {
    /// key space
    fn key_space(&self) -> &str;

    /// Table name
    fn table_name(&self) -> &str;

    /// CQL for table creation
    fn create_table_cql(&self) -> &str;

    /// CQL for drop table
    fn drop_table_cql(&self) -> &str;
}

