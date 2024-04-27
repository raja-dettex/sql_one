use sql_one_execution::table::{table, ColumnInfo};
use sql_one_parser::{commands::{create::{Column, SqlTypeInfo}, select_condition::{Condition, SelectStatementCondition}}, parser::Parse, value::Value};
use bigdecimal::FromPrimitive;

fn main() {
    let mut table = table::new(ColumnInfo { columns : vec![
        Column { name : "id".to_string(), type_info: SqlTypeInfo::Int},
        Column { name: "name".to_string(), type_info : SqlTypeInfo::String}
    ]});
    table.insert(vec![Value::Number(bigdecimal::BigDecimal::from_i32(1).expect("value")), Value::String("raja".to_string())]);
    table.insert(vec![Value::Number(bigdecimal::BigDecimal::from_i32(2).expect("value")), Value::String("neha".to_string())]);
    let condition = Some(Condition { 
        first: "id".to_string(), 
        second : "1".to_string(),
        token: "!=".to_string()
    });
    let query_str = "select id, name from User where id != 1;".to_string();
    let query = SelectStatementCondition::parse_from_raw( &query_str).unwrap().1;
    assert_eq!(query.where_clause, condition);
    let table_iter = table.select(vec!["id".to_string(), "name".to_string()], condition).unwrap();
    for values in table_iter { 
        println!("Row is {:#?}", values);
    }
}
