
#[cfg(test)]
mod CreateTest { 

    use crate::{commands::create::{Column, CreateStatement, SqlTypeInfo}, parser::Parse};

    use super::*;
    
    #[test]
    fn test_create() { 
        let expected = CreateStatement {
            table: "foo".to_string(),
            columns : vec![
                Column{name: "col1".to_string(), type_info: SqlTypeInfo::Int},
                Column{name: "col2".to_string(), type_info: SqlTypeInfo::String}
            ]
        };
        let actual = CreateStatement::parse_from_raw("CREATE TABLE foo (col1 int, col2 string)").unwrap().1;
        println!("actual is {:#?} expected is {:#?}", actual, expected);
        assert_eq!(actual, expected);
    }
}