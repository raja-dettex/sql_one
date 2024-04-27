use core::fmt;

use nom::{bytes::complete::{tag, take_until, take_while}, character::complete::{multispace0, multispace1}, combinator::{cond, map, map_res, opt}, error::context, sequence::{pair, preceded, terminated, tuple}};
use nom_supreme::{tag::{ complete::tag_no_case}, ParserExt};
use serde::{Deserialize, Serialize};

use crate::{error::MyParseError, parser::{comma_sep, identifier, Parse, ParseResult, RawSpan}};


#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SelectStatementCondition {
    pub table: String,
    pub fields: Vec<String>,
    pub where_clause: Option<Condition>, // Add a where_clause field
}


#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Condition { 
    pub first : String, 
    pub second : String,
    pub token : String
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl fmt::Display for SelectStatementCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT ")?;

        write!(f, "{}", self.fields.join(", "))?;

        write!(f, " FROM ")?;

        write!(f, "{}", self.table)?;

        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {}", where_clause)?;
        }

        Ok(())
    }
}

impl<'a> Parse<'a> for SelectStatementCondition {
    fn parse(input: RawSpan<'a>) -> ParseResult<'a, Self> {
        let (remaining_input, (_, _, fields, _, _, _, table, where_clause)) = context(
            "Select Statement",
            tuple((
                tag_no_case("select"),
                multispace1,
                comma_sep(identifier).context("Select Columns"),
                multispace1,
                tag_no_case("from"),
                multispace1,
                identifier.context("From Table"),
                //multispace1,
                opt(preceded(
                    multispace1,
                    pair(
                        tag_no_case("where"),
                        map_res(
                            preceded(multispace1, take_until(";")),
                            |clause: RawSpan<'a>| Ok::<String, String>(clause.fragment().to_string()), // Return clause as string
                        ),
                    ),
                )),
            )),
        )(input)?;

        //let where_clause = where_clause.map(|span| span.to_string());
        let where_clause_condition: Option<String>  = where_clause.map(|(_, clause)| clause);
        
        if let Some(clause) = where_clause_condition { 
            if clause.contains("!=") { 

                let parts : Vec<&str>  = clause.split("!=").collect();
                let moved_parts : Vec<String>= parts.iter().map(|part|  {
                    let trimmed = part.trim();
                    trimmed.to_string()
                }).collect();
                let condition = Condition { 
                    first: moved_parts.get(0).unwrap().to_string(),
                    second: moved_parts.get(1).unwrap().to_string(),
                    token : "!=".to_string()
                };
                return Ok((
                    remaining_input,
                    SelectStatementCondition {
                        fields,
                        table,
                        where_clause: Some(condition),
                    },
                ));
            }
            else if clause.contains("=") { 
                println!("Clause is {}", clause);
                let parts : Vec<String>  = clause.split("=").map(|part| part.trim().to_string()).collect();

                let condition = Condition { 
                    first: parts.get(0).unwrap().to_string(),
                    second: parts.get(1).unwrap().to_string(),
                    token : "=".to_string()
                };
                return Ok((
                    remaining_input,
                    SelectStatementCondition {
                        fields,
                        table,
                        where_clause: Some(condition),
                    },
                ));
            }
            
            else if clause.contains("<") { 
                let parts : Vec<String>  = clause.split("<").map(|part| part.trim().to_string()).collect();
                let condition = Condition { 
                    first: parts.get(0).unwrap().to_string(),
                    second: parts.get(1).unwrap().to_string(),
                    token : "<".to_string()
                };
                return Ok((
                    remaining_input,
                    SelectStatementCondition {
                        fields,
                        table,
                        where_clause: Some(condition),
                    },
                ));
            }
            else if clause.contains(">") { 
                let parts : Vec<String>  = clause.split(">").map(|part| part.trim().to_string()).collect();
                let condition = Condition { 
                    first: parts.get(0).unwrap().to_string(),
                    second: parts.get(1).unwrap().to_string(),
                    token : ">".to_string()
                };
                return Ok((
                    remaining_input,
                    SelectStatementCondition {
                        fields,
                        table,
                        where_clause: Some(condition),
                    },
                ));
            }
        }
        Ok((
            remaining_input,
            SelectStatementCondition {
                fields,
                table,
                where_clause: None,
            },
        ))

        // Trim the trailing whitespace before semicolon
        //let where_clause = where_clause.map(|clause| clause.trim_end_matches(|c| c == ';' || c == ' ').to_string());


        
    }
}




#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_where() { 
        let input = "select foo from boo where name = srinia;";
        let expected = SelectStatementCondition { 
            table : "boo".to_string(),
            fields : vec!["foo".to_string()],
            where_clause: Some(Condition { 
                first: "name".to_string(),
                second : "srinia".to_string(),
                token: "=".to_string()
            })
        };
        assert_eq!(SelectStatementCondition::parse_from_raw(input).unwrap().1, expected);
    }


    #[test]
    fn test_select_without_where() {
        let input = "SELECT foo, bar FROM t1;";
        let expected = SelectStatementCondition {
            table: "t1".to_string(),
            fields: vec!["foo".to_string(), "bar".to_string()],
            where_clause: None,
        };

        assert_eq!(
            SelectStatementCondition::parse_from_raw(input).unwrap().1,
            expected
        );
    }
}
