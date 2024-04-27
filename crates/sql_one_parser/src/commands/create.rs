use nom::{branch::alt, bytes::complete::{tag, take_while1}, character::complete::{multispace1,char}, combinator::map, error::context, multi::separated_list1, sequence::{preceded, separated_pair, tuple}, IResult};
use nom_locate::LocatedSpan;
use serde::{Deserialize, Serialize};
use nom_supreme::{parser_ext, tag::complete::tag_no_case, ParserExt}; // Added ParserExt here
use crate::parser::{RawSpan, ParseResult, Parse, identifier,comma_sep};


#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, derive_more::Display)]
pub enum SqlTypeInfo { 
    String,
    Int
}

impl<'a> Parse<'a> for SqlTypeInfo {
    fn parse(input : RawSpan<'a>) -> ParseResult<'a, Self> {
        context(
            "Column_Type", 
            alt((
                map(tag_no_case("string"), |_| Self::String),
                map(tag_no_case("int"), |_| Self::Int)
            ))
        )(input)
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_info: SqlTypeInfo,
}

// parses "<colName> <colType>"
impl<'a> Parse<'a> for Column {
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        context(
            "Create Column",
            map(
                separated_pair(
                    identifier.context("Column Name"),
                    multispace1,
                    SqlTypeInfo::parse,
                ),
                |(name, type_info)| Self { name, type_info },
            ),
        )(input)
    }
}

/// The table and its columns to create
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CreateStatement {
    pub table: String,
    pub columns: Vec<Column>,
}

// parses a comma seperated list of column definitions contained in parens
fn column_definitions(input: RawSpan<'_>) -> ParseResult<'_, Vec<Column>> {
    context(
        "Column Definitions",
        map(
            tuple((char('('), comma_sep(Column::parse), char(')'))),
            |(_, cols, _)| cols,
        ),
    )(input)
}

// parses "CREATE TABLE <table name> <column defs>
impl<'a> Parse<'a> for CreateStatement {
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        map(
            separated_pair(
                // table name
                preceded(
                    tuple((
                        tag_no_case("create"),
                        multispace1,
                        tag_no_case("table"),
                        multispace1,
                    )),
                    identifier.context("Table Name"),
                ),
                multispace1,
                // column defs
                column_definitions,
            )
            .context("Create Table"),
            |(table, columns)| Self { table, columns },
        )(input)
    }
}