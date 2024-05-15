

use std::{collections::HashMap, fs::{self, File}, hash::Hash, io::Write};

use miette::Diagnostic;
use serde::{Deserialize, Serialize};
use sql_one_flexi_engine::{page::table::{key_type, TableMetaData}, storage::Storage};
use sql_one_parser::{ast::{parse_sql_query, SqlQuery}, commands::create::SqlTypeInfo};

use crate::{error::{QueryExecutionError, SQLError}, table::{table, ColumnInfo, Row, TableIter}};
use derive_more::Display;
use std::io::Read;
use thiserror::Error;

#[derive(Debug,Display )]
pub enum ExecResponse<'a> { 
    #[display(fmt = "{_0:?}")] 
    Select(TableIter<'a>),
    Insert,
    Crete
}




#[derive(Clone, Serialize , Deserialize, Debug)]
pub struct Execution { 
    pub tables : HashMap<String, table>
}

impl Execution { 
    pub fn new() -> Self { 
        match Self::retrieve_from_json() {
            Ok(s) => s,
            Err(err) =>  { 
                println!("error : {}", err);
                Self{tables: HashMap::new()}
            },
        }
        
    }

    pub fn get_table(&self, name: &str) -> table { 
        self.tables.get(name).unwrap().clone()
    }

    pub fn invoke_storage_metadata(&mut self)  { 
      
        for (name, _) in self.tables.clone() { 
            let storage = Storage::new(None, format!("{}_storage.json", name));
       
            let mut table = self.tables.get_mut(&name).unwrap();
            table.storage = storage;
           
        }
    } 

    fn retrieve_from_json() -> Result<Self, String>  {
        match fs::metadata("./execution.json") {
            Ok(metadata) => { 
                if metadata.is_file() {
                    match File::open("./execution.json") {
                        Ok(mut file) => {
                            let mut str = String::new();
                            match file.read_to_string(&mut str) {
                                Ok(_) => { 
                                    let mut s : Self = serde_json::from_str(&str).unwrap();
                                    s.invoke_storage_metadata();
                                
                                    Ok(s)
                                },
                                Err(err) => Err(err.to_string()),
                            }

                        }
                        Err(err) =>Err(err.to_string()),
                    }

                    
                } else { 
                    Err("file does not exist".to_string())
                }
            },
            Err(_) => { 
                Err("Failed to read execution state metadata".to_string())
            },
        }
    }

    fn save_to_json(&mut self) -> Result<(), String>{
        match serde_json::to_string(&self) {
            Ok(exec_str) => { 
                match File::create("./execution.json") {
                    Ok(mut file) => { 
                        file.write_all(exec_str.as_bytes()).map_err(|err| err.to_string())
                    },
                    Err(err) => Err(err.to_string()),
                }
            },
            Err(err) => Err(err.to_string()),
        }
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
                println!("in insert");
                let Some(table) = self.tables.get_mut(&insert.table) else { 
                    return Err(QueryExecutionError::TableNotFound(insert.table))
                };
                let values : Vec<String>= insert.values.iter().map(|val| val.to_string()).collect();
                table.insert(insert.values);
                Ok(ExecResponse::Insert)
            },
            SqlQuery::Create(create) => { 
                let columns  = create.columns;
                let primary_key = columns[0].clone().name;
                let mut prim_key_type: key_type;
                if columns[0].type_info == SqlTypeInfo::String { 
                    prim_key_type = key_type::Strings;
                } else  { 
                    prim_key_type = key_type::Number
                }
                let table_metadata = TableMetaData::new(create.table.clone(), primary_key, prim_key_type);
                let table = table::new(ColumnInfo::new(columns), table_metadata);
                self.tables.insert(create.table, table);
                match self.save_to_json() {
                    Ok(_) => println!("execiton state saved to disk"),
                    Err(err) => println!("error saving to execution state {}", err),
                }
                Ok(ExecResponse::Crete)
            },
        }
    } 
}

