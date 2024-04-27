use sql_one_parser::{commands::select_condition::SelectStatementCondition, parser::Parse};



fn main() {
    let str = "id = 4";
    let trimmed = str.trim();
    println!("trimmed : {}", trimmed);
    // println!("hey");
    // let input = "select hello from foo;";
    // let parsed = SelectStatementCondition::parse_from_raw(input);
    // println!("parsed result : {:#?}", parsed);
    // let another_input = "select hello from foo WHERE id = 4;";
    // let another_parsed = SelectStatementCondition::parse_from_raw(input).unwrap().1;
    // println!("another parsed : {:#?}", another_parsed);
    let input = "select foo, bar from t1 where id != 4;";
        // let expected = SelectStatementCondition {
        //     table: "t1".to_string(),
        //     fields: vec!["foo".to_string(), "bar".to_string()],
        //     where_clause: Some("condition".to_string()),
        // };
        println!("parsed : {:#?}", SelectStatementCondition::parse_from_raw(input).unwrap().1);
        // assert_eq!(
        //     SelectStatementCondition::parse_from_raw(input).unwrap().1,
        //     expected
        // );
}

