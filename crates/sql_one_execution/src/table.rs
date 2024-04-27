use serde::{Serialize, Deserialize};
use sql_one_parser::commands::create::{Column, SqlTypeInfo};
use sql_one_parser::commands::select_condition::Condition;
use sql_one_parser::value::Value;
use std::collections::{btree_map, BTreeMap, HashMap};
use std::rc::Rc;

use crate::error::QueryExecutionError;


type StoredRow = HashMap<String,Value>;

#[derive(Debug, Default, Serialize, Deserialize, derive_more::From, Clone)]
pub struct ColumnInfo { 
    pub columns : Vec<Column>
}

impl ColumnInfo { 

    pub fn new(columns : Vec<Column>) -> Self { 
        Self {columns}
    }
    pub fn iter(&self) -> impl Iterator<Item = &Column> { 
        self.columns.iter()
    } 

    pub fn find_column(&self, column_name : &String) -> Result<&Column, QueryExecutionError> { 
        self.iter().find(|col| col.name == *column_name).ok_or_else(|| QueryExecutionError::ColumnNotFound(column_name.to_owned()))
    }
}



pub struct table { 
    rows : BTreeMap<usize, StoredRow>, 
    columns : ColumnInfo, 
    filtered_rows : BTreeMap<usize, StoredRow>
}


#[derive(Debug, Clone)]
pub struct Row<'a> { 
    pub id : usize, 
    pub columns : Rc<ColumnInfo>,
    pub data : HashMap<&'a String, &'a Value>
}

impl<'a> Row<'a> { 
    pub fn new(id : usize, columns : Rc<ColumnInfo>, data:  HashMap<&'a String, &'a Value>) -> Self { 
        Self{id, columns, data}
    }
}

#[derive(Debug , Clone)]
pub struct TableIter<'a> { 
    map_iter: std::collections::btree_map::Iter<'a, usize, StoredRow>, 
    columns : Rc<ColumnInfo>
}


impl<'a> TableIter<'a> { 
    pub fn new(map_iter : std::collections::btree_map::Iter<'a, usize, StoredRow>, 
            columns : Rc<ColumnInfo>) -> Self { 
                Self{map_iter, columns}
    }

    
}

impl<'a> Iterator for TableIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.map_iter
            .next()
            .map(|(id, data)| { 
                let projected_data  = data.iter()
                    .filter_map(|(key, value) | self.columns.find_column(key).ok().map(|_|(key, value)))
                    .collect();
                Row::new(*id, self.columns.clone(), projected_data)
            })
    }
}

impl<'a> IntoIterator for &'a table {
    type Item = Row<'a>;

    type IntoIter = TableIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let col_info = Rc::new(self.columns.clone());
        TableIter::new(self.rows.iter(), col_info)
    }
}


impl table { 

    pub fn iter(&self ) -> impl Iterator<Item = Row> { 
        self.into_iter()
    }
    pub fn new(columns : ColumnInfo ) -> Self { 
        table { 
            rows : BTreeMap::new(),
            columns: columns, 
            filtered_rows: BTreeMap::new()
        }
    }

    pub fn from(columns: ColumnInfo, data : BTreeMap<usize, StoredRow> ) -> Self { 
        table { 
            rows : data, 
            columns,
            filtered_rows: BTreeMap::new()
        }
    }

    pub fn filter_rows<'a>(&'a mut self, conditions : Condition)  { 
        let (condition_key , condition_value, token ) = { 
            let k = conditions.first;
            let v = Value::value(conditions.second);
            (k, v, conditions.token)
        };
        let filtered_rows : BTreeMap<usize, StoredRow>= self.rows.iter().filter(move |(_, row)| { 
            row.iter().any(|(key , value) | { 
                (token == "=".to_string() && key == &condition_key && *value == condition_value) || 
                (token == "!=".to_string() && key == &condition_key && *value != condition_value)
            })
        }).map(|(id, row)| (*id, row.clone())).collect();
        self.filtered_rows = filtered_rows;
       
    }

    pub fn select(&mut self, columns : Vec<String>, clause : Option<Condition>) -> Result<TableIter, QueryExecutionError> { 
        let selected_cols : Result<Vec<_>, _> = columns.into_iter()
            .map(|col_name| { 
                self.columns.find_column(&col_name).map(|col| col.clone())
            }).collect();
        let column_rc : Rc<ColumnInfo> = Rc::new(selected_cols.unwrap().into());
        let rows : BTreeMap<usize, StoredRow>;
            match clause { 
                Some(condition) => { 
                    self.filter_rows(condition);
                    Ok(TableIter::new(self.filtered_rows.iter(), column_rc))
                    
                }, 
                None => Ok(TableIter::new(self.rows.iter(),column_rc))
            }
        
    } 

    pub fn insert(&mut self, values : Vec<Value>) -> Result<(), QueryExecutionError> { 
        let id = self.rows.last_key_value().map_or(0, |(max_id, _)| (max_id + 1));
        let row = values
            .into_iter()
            .zip(self.columns.iter())
            .map(|(value, col)| match (col.to_owned().type_info, value) {
                (SqlTypeInfo::String, v @ Value::String(_)) => Ok((col.name.to_owned(), v)),
                (SqlTypeInfo::Int, v @ Value::Number(_)) => Ok((col.name.to_owned(), v)), // TODO: when we add floats make sure number is an int
                (_,v) => Err(QueryExecutionError::InsertTypeMismatch(col.to_owned().type_info, v)),
            })
            .collect::<Result<HashMap<_, _>,_>>()?;

        self.rows.insert(id, row.into());
        Ok(())
    }
    pub fn travserse(&self) { 
        for (key , val) in self.rows.iter() { 
            println!("key is {}, val is {:#?}", key, val);
        }
    }
}


#[cfg(test)]
mod tests {
    use bigdecimal::FromPrimitive;
    use sql_one_parser::{commands::{create::{Column, SqlTypeInfo}, select_condition::Condition}, value::Value};

    use super::{table, ColumnInfo};


    #[test] 
    fn test_table_insertion() { 
        let mut table = table::new(ColumnInfo { columns : vec![
            Column{ name : "foo".to_string(), type_info: SqlTypeInfo::String}, 
            Column{name : "hoo".to_string(), type_info : SqlTypeInfo::Int}
        ]});
        table.insert(vec![Value::String("hello".to_string()), Value::Number(bigdecimal::BigDecimal::from_i32(1).expect("value"))]);
        table.travserse();
    }


    fn test_table_conditional_select() { 
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
        let table_iter = table.select(vec!["id".to_string(), "name".to_string()], condition).unwrap();
    }
}