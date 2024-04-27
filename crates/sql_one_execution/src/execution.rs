

use std::{collections::HashMap, hash::Hash};

use miette::Diagnostic;
use sql_one_parser::ast::{parse_sql_query, SqlQuery};

use crate::{error::{QueryExecutionError, SQLError}, table::{table, ColumnInfo, Row, TableIter}};
use derive_more::Display;
use thiserror::Error;

#[derive(Debug,Display )]
pub enum ExecResponse<'a> { 
    #[display(fmt = "{_0:?}")] 
    Select(TableIter<'a>),
    Insert,
    Crete
}




pub struct Execution { 
    tables : HashMap<String, table>
}

impl Execution { 
    pub fn new() -> Self { 
        Self{tables: HashMap::new()}
    }

    pub fn parse_and_run<'a>(&mut self, query_str : &'a str) -> Result<ExecResponse, SQLError<'a>> { 
        let query = parse_sql_query(&query_str);
        match query { 
            Ok(q) => { 
                match self.run(q) {
                    Ok(res) => Ok(res),
                    Err(err) => Err(SQLError::QueryExecutionError(err)),
                }
            }, 
            Err(e) => Err(SQLError::ParsingError(e))
        }
    }

    pub fn run(&mut self, query : SqlQuery) -> Result<ExecResponse, QueryExecutionError> { 
        match query {
            SqlQuery::Select(mut select) =>  {
                let table_name = select.table;
                let  mut table = self.tables.get_mut(&table_name).ok_or(QueryExecutionError::TableNotFound(table_name));
                match table { 
                    Ok(mut t) => { 
                        let s = t.select(select.fields.clone(), select.where_clause.clone()).unwrap();
                        //let rows : Vec<Row>  = t.iter().collect();
                        Ok(ExecResponse::Select(t.select(select.fields, select.where_clause)?))
            
                    },
                    Err(err) => Err(err)
                }
                },
            SqlQuery::Insert(insert) => {
                let Some(table) = self.tables.get_mut(&insert.table) else { 
                    return Err(QueryExecutionError::TableNotFound(insert.table))
                };
                let values : Vec<String>= insert.values.iter().map(|val| val.to_string()).collect();
                table.insert(insert.values);
                Ok(ExecResponse::Insert)
            },
            SqlQuery::Create(create) => { 
                let columns  = create.columns;
                let table = table::new(ColumnInfo::new(columns));
                self.tables.insert(create.table, table);
                Ok(ExecResponse::Crete)
            },
        }
    } 
}

