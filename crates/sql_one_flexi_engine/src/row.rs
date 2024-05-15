use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use sql_one_parser::value::Value;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct StoredRow { 
    pub row : HashMap<String, Value>
}


impl StoredRow { 
    pub fn new(row : HashMap<String, Value>) -> Self { 
        Self{row}
    }
}

