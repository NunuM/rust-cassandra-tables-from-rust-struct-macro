#![recursion_limit = "128"]
extern crate proc_macro;

use proc_macro::TokenStream;
use std::collections::{BTreeMap, HashMap};

use syn;
use syn::NestedMeta;

use quote::{quote, ToTokens};

#[proc_macro_derive(Cassandra, attributes(column, table))]
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

                        table_meta.set_keypsace(&key_space);
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

    let ident = &ast.ident;

    // Helper is provided for handling complex generic types correctly and effortlessly
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    let impl_ast = quote!(
        impl #impl_generics Cassandra for #ident #ty_generics #where_clause {

            fn create_table_cql(&self) -> String {
                format!("{}", #create_table_sql )
            }

            fn drop_table_cql(&self) -> String {
                format!("{}", #drop_table_sql )
            }

            fn key_space(&self) -> &str {
                #key_space
            }

            fn table_name(&self) -> &str {
                #table_name
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

    fn set_keypsace(&mut self, key_space: &String) {
        self.key_space = key_space.to_owned();
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

        let create_stmt = format!("CREATE TABLE IF NOT EXISTS {}.{}", self.key_space, self.name);

        if c_keys.len() > 0 {
            format!("{} ( {}, PRIMARY KEY (({}), {}) ) {};", create_stmt, columns, primary_keys, c_keys.join(","), table_options)
        } else {
            format!("{} ({}, PRIMARY KEY ({}) ) {};", create_stmt, columns, primary_keys, table_options)
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
                                    // email, url, phone, credit_card, non_control_character
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
