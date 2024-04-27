use std::str::FromStr;

use bigdecimal::BigDecimal;
use derive_more::Display;
use nom::{
    branch::alt,
    bytes::complete::{take_until, take_while},
    character::complete::multispace0,
    error::context,
    sequence::{preceded, terminated, tuple},
    Parser,
};
use nom_supreme::tag::complete::tag;
use serde::{Deserialize, Serialize};

use crate::parser::{peek_then_cut, Parse, ParseResult, RawSpan};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum Value {
    Number(BigDecimal), // TODO: should we make literals for ints vs floats?
    String(String),
}

impl Value { 
    pub fn value(val : String) -> Self { 
        match BigDecimal::from_str(val.as_str()).map_err(|err|err.to_string()) {
            Ok(decimal) => Value::Number(decimal),
            Err(err) => { 
                println!("err : {}", err);
                Value::String(val)
            },
        }
    }
}

/// Parse a single quoted string value
fn parse_string_value(input: RawSpan<'_>) -> ParseResult<'_, Value> {
    // TODO: look into https://github.com/rust-bakery/nom/blob/main/examples/string.rs
    // for escaped strings
    let (remaining, (_, str_value, _)) = context(
        "String Literal",
        tuple((
            tag("'"),
            take_until("'").map(|s: RawSpan| Value::String(s.fragment().to_string())),
            tag("'"), // take_until does not consume the ending quote
        )),
    )(input)?;

    Ok((remaining, str_value))
}

/// Parse a numeric literal
fn parse_number_value(input: RawSpan<'_>) -> ParseResult<'_, Value> {
    let (remaining, digits) =
        context("Number Literal", take_while(|c: char| c.is_numeric()))(input)?; // TODO: handle floats

    let digits = digits.fragment();

    Ok((
        remaining,
        Value::Number(BigDecimal::from_str(digits).unwrap()),
    ))
}

impl<'a> Parse<'a> for Value {
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        context(
            "Value",
            preceded(
                multispace0,
                terminated(
                    alt((peek_then_cut("'", parse_string_value), parse_number_value)),
                    multispace0,
                ),
            ),
        )(input)
    }
}

#[cfg(test)]
mod tests {
    use bigdecimal::{BigDecimal, FromPrimitive};

    use super::Value;


    #[test] 
    fn test_value_string() { 
        let str = "name".to_string();
        let expected = Value::String(str.clone());
        assert_eq!(Value::value(str), expected)

    }

    #[test]
    fn test_value_decimal() { 
        let str = "4".to_string();
        let expected = Value::Number(BigDecimal::from_i32(4).unwrap());
        assert_eq!(Value::value(str), expected)
        
    }
}
