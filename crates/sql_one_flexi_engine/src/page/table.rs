use crate::row::StoredRow;
use serde::{Deserialize, Serialize};
use sql_one_parser::value::Value;

use super::page::PAGE_SIZE;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetaData { 
    pub table_name : String, 
    //page_number : String,
    pub primary_key : String,
    pub prim_key_type : key_type
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum key_type { 
    Number,
    Strings
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowMetaData { 
    pub table : TableMetaData,
    pub primary_key : Value, 
    pub row_size : usize,
    pub range : Vec<usize>,
    pub page_number : usize
}
#[derive(Debug, Clone , Serialize, Deserialize)]
pub struct PageData { 
    pub page_number : usize,
    pub current_size : usize,
    pub max_size : usize
}


impl TableMetaData { 
    pub fn new(table_name : String, primary_key : String, prim_key_type: key_type) -> Self { 
        Self{table_name, primary_key, prim_key_type}
    }
}

impl PageData { 
    pub fn default(page_number: usize) -> Self { 
        Self{page_number: 1, current_size: 0, max_size: PAGE_SIZE}
    }
    pub fn getChunkData(&mut self, row_size : usize) -> (usize, Vec<usize>) { 
        if(self.isFull(row_size)) { 
            self.page_number = self.page_number + 1;
            self.current_size = 0;
            return (self.page_number , vec![self.current_size, self.current_size + row_size - 1]);
        }
        let size = self.current_size;
        self.current_size = self.current_size + row_size;
        (self.page_number , vec![size, size + row_size - 1])
    }

    pub fn isFull(&self, current_size : usize) -> bool { 
        self.current_size + current_size > self.max_size
    } 

    pub fn rollBackCurrentSize(&mut self, size : usize)  {
        self.current_size = self.current_size - size;
    } 
}


impl RowMetaData { 
    pub fn new(table : TableMetaData, primary_key : Value,  row_size: usize, range : Vec<usize>, page_number : usize) -> Self { 
        Self{table, primary_key, row_size, range, page_number}
    }
}

//#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bigdecimal::{BigDecimal, FromPrimitive};
    use crate::row::StoredRow;
    use sql_one_parser::value::Value;

    use crate::page::serializer::RowSerializer;

    use super::{key_type, PageData, RowMetaData, TableMetaData};


    //#[test] 
    pub fn sample_test() { 
        let mut page_data = PageData::default(1);
        let table_data = TableMetaData::new("users".to_string(), "user_id".to_string(), key_type::Number);
        let mut rows = HashMap::new();
        rows.insert("id".to_string(), Value::Number(BigDecimal::from(1)));
        rows.insert("name".to_string(), Value::String("raja".to_string()));
        let row = StoredRow::new(rows);
        let chunk = row.to_bytes().unwrap();
        let (page_number, chunk_range) = page_data.getChunkData(chunk.size);
        let row_data = RowMetaData::new(table_data, Value::Number(BigDecimal::from_i16(1).unwrap()),  chunk.size, chunk_range, page_number);
        println!("row meta data : {:#?}", row_data);
    }
}