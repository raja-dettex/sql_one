use std::{collections::{btree_map, BTreeMap, HashMap}, fmt::format, fs::{self, File}, io::{Read, Write}, panic::RefUnwindSafe};


use serde::{Deserialize, Serialize};
use sql_one_parser::{commands::select_condition::Condition, value::Value};

use crate::{page::{self, error::InternalStorageError, page::{Page, PAGE_SIZE}, serializer::RowSerializer, table::{key_type, PageData, RowMetaData, TableMetaData}}, row::StoredRow};

#[derive(Clone, Debug)]
struct Temp { 
    value : Value,
    row : RowMetaData
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Storage { 
    pub table_metadata : TableMetaData,
    pub pages : Page, 
    pub page_metadata : PageData,
    pub rows : btree_map::BTreeMap<Value, RowMetaData>,
    pub file_name : String
}
      

impl Storage { 
    pub fn new(table_metadata : Option<TableMetaData>, file_name : String) -> Self {
        match fs::metadata(file_name.clone()) {
            Ok(metadata) => { 
                if metadata.is_file() {
                    match File::open(file_name.clone()) {
                        Ok(mut file) => {
                            let mut str = String::new();
                            match file.read_to_string(&mut str) {
                                Ok(_) => { 
                                    let s : Self = serde_json::from_str(&str).unwrap();
                                    s
                                },
                                Err(err) => panic!("panicked at storage : {:?}", err),
                            }

                        }
                        Err(err) => panic!("panicked at storage : {:?}", err),
                    }

                    
                } else { 
                    Self::from_table_meta(table_metadata.unwrap(), file_name.clone())
                }
            },
            Err(_) => { 
                Self::from_table_meta(table_metadata.unwrap(), file_name.clone())
            },
        }
        //let table_metadata = TableMetaData::new()
        
    }

    
    pub fn from_table_meta(table_metadata : TableMetaData, file_name : String) -> Self {
        let pages = Page::new(1, Vec::new());
        let page_metadata = PageData::default(1);
        let rows = btree_map::BTreeMap::new();
        Self { table_metadata ,  pages, page_metadata , rows, file_name}
    }
    pub fn default() -> Self {
        let pages = Page::new(1, Vec::new());
        let page_metadata = PageData::default(1);
        let rows = btree_map::BTreeMap::new();
        let t_meta = TableMetaData::new(
            "User".to_string(), 
            "id".to_string(),
            key_type::Number
        );
        Self { table_metadata : t_meta.clone(),  pages, page_metadata , rows, file_name : format!("{}_storage.json", t_meta.table_name.clone())}
    }
    pub fn save_to_json(&self) -> Result<(), String> { 
        let serialized_storage = serde_json::to_string(self).unwrap();
        let mut file = File::create(self.file_name.clone()).unwrap();
        file.write_all(serialized_storage.as_bytes()).map_err(|err| err.to_string())
    }

    pub fn isPageFull(&self, chunk_size: usize) -> bool { 

        if self.pages.data.len() == PAGE_SIZE { 
            return true;
        };

        self.pages.data.len() + chunk_size > PAGE_SIZE
    }

    // pub fn page_exist(&self) -> bool { 
    //     self.pages.len() > 0
    // }

    pub fn create_new_page(&mut self, page_number : usize) {
        let page = Page::default(page_number);
        self.pages = page;
    }

    pub fn read(&mut self, prim_key_value : Value) -> Result<StoredRow, InternalStorageError> {
        let row = self.rows.get(&prim_key_value);
        match row {
            Some(value) => {
                match  Page::read_chunks(value.page_number, value.range.clone(), value.table.table_name.clone()) {
                    Some(bytes) => {
                        match  StoredRow::from_bytes(&bytes.clone())  {
                            Ok(row) => Ok(row),
                            Err(err) => Err(InternalStorageError::SerializerError(err)),
                        }
                    },
                    None => Err(InternalStorageError::ErrReadFromDisk("unable to read from disk".to_string()))
                }
            },
            None => Err(InternalStorageError::ErrInternal("row metadata not found".to_string()))
        }
    }

    pub fn filter_row(&mut self, val : Vec<Value> ) -> BTreeMap<Value, RowMetaData> { 
        let mut row_temp_map = BTreeMap::new();
        for v in val { 
            let mut index: usize = 0;
            for (i, (value, row) ) in self.rows.iter().enumerate() { 
                if *value == v { 
                    index = i;
                }
            }
            let mut temp = Vec::<Temp>::new();
            for ( i , (value , row) ) in self.rows.iter().enumerate() { 
                if i >= index { 
                    temp.push(Temp{value : value.clone(), row : row.clone()})
                }
            }
            println!("temp is {:?}", temp);
            

            let mut another_temp = Vec::new();
        
            if index == temp.len() - 1 { 
                for i in  0..=temp.len() - 2 { 
                    another_temp.push(temp.get(i).unwrap().clone());
                }
            } else if index != temp.len() - 1 { 
                let mut i = index.clone();
                while(i <= temp.len() - 2) { 
                    let t = temp.get(i).unwrap();
                    let next_t = temp.get(i + 1).unwrap();
                    
                    let next_lower_bound = t.row.range[0];
                    let next_upper_bound = next_t.row.range[1] - next_t.row.range[0] + t.row.range[0];
                    let next_row = RowMetaData { table : self.table_metadata.clone(), primary_key: next_t.value.clone(), row_size: next_t.row.row_size, range : vec![next_lower_bound, next_upper_bound], 
                                                            page_number : next_t.row.page_number};  
                    another_temp.push(Temp{value : next_t.value.clone(), row : next_row});
                    i += 1;
                }
            }
            
            //let mut row_temp_map = BTreeMap::new();
            for temp in another_temp  {
                row_temp_map.insert(temp.value, temp.row );
            }
        }
        
        row_temp_map
    }
 
    pub fn read_when(&mut self, conditions : Option<Condition>) -> Vec<StoredRow> {
        let mut rows = self.read_all();
        if let Some(condition) = conditions { 
            let mut required_rows = Vec::new();
            for row in rows {
                for (key, value) in row.row.iter() {
                    if *key == condition.first && *value == Value::value(condition.second.clone()) && condition.token == "=".to_string() { 
                        required_rows.push(row.clone());
                    } else if *key == condition.first && *value != Value::value(condition.second.clone()) && condition.token == "!=".to_string() { 
                        required_rows.push(row.clone());
                    } 
                }
            }
            return required_rows;
        }
        rows

    }


    pub fn read_all(&mut self) -> Vec<StoredRow> { 
        let mut rows = Vec::new(); 
        
        for (index , value) in self.rows.iter() { 
            if let Some(value) = Page::read_chunks(value.page_number, value.range.clone(), value.table.table_name.clone()) { 
                
                if let Ok(row) = StoredRow::from_bytes(&value.clone()) { 
                    rows.push(row);
                } else if let Err(err) = StoredRow::from_bytes(&value.clone()){ 
                    println!("error is {:#?}" , err);
                    continue
                }
            } else { 
                continue
            }
         }

        rows
    }

    pub fn delete(&mut self , conditions : Option<Condition> ) { 
        // deleting from disk means reclaiming the disk space occupied by the given row
        // bringing the page into the buffer
        // scan the btree map to select the rows to be deleted 
        match conditions {
            Some(condition) => {
                let mut to_be_deleted_row_metadata = BTreeMap::new();
                //let mut updated_row_metadata = BTreeMap::new();
                // filter the btree to find out which rows will be deleted
                let to_be_deleted_rows = self.read_when(Some(condition));
                
                for stored_row in to_be_deleted_rows { 
                    for (key, value) in stored_row.row  { 
                    
                        if key == self.table_metadata.primary_key { 
                                let (key , value) = self.rows.get_key_value(&value).unwrap();
                                to_be_deleted_row_metadata.insert(key.clone(), value.clone());
                            
                        } 
                    }
                }
                
                //let mut pages = Vec::new();
                for (value, row) in to_be_deleted_row_metadata.clone() { 
                    //let page = self.pages.get(row.page_number - 1).unwrap();
                    if self.pages.page_number == row.page_number { 
                        self.pages = self.pages.delete_chunks(row.range[0], row.range[1], true, self.table_metadata.table_name.clone());
                    }
                    //pages.push(page);
                    //self.rows.remove(&value);
                }
                //self.pages = pages.clone();
                self.page_metadata.page_number = self.pages.page_number;
                self.page_metadata.current_size = self.pages.data.len() - 1;
                let values = to_be_deleted_row_metadata.iter().map(|(val, _)| val.clone()).collect();
                self.rows = self.filter_row(values);
                self.save_to_json();
                //self.rows = updated_row_metadata;

            }, 
            None => { 
                // delete all the rows of a given table
                let page_numbers: Vec<usize> = self.rows.iter().map(|(key , value)| { 
                    value.page_number
                }).collect();
                for page in page_numbers { 
                    Page::delete(page, self.table_metadata.table_name.clone());
                }
                self.pages = Page::new(1, Vec::new());
                self.page_metadata = PageData::default(1);
                self.rows = BTreeMap::new();
                self.save_to_json();
            }
        }
    }

    pub fn write(&mut self, data : StoredRow)  -> Result<&str, InternalStorageError>{ 

        match data.row.get(&self.table_metadata.primary_key) {
            Some(key) => {
                match data.to_bytes() {
                    Ok(chunk) => { 
                        let ( page_number, chunk_range) = self.page_metadata.getChunkData(chunk.size);
                        if self.isPageFull(chunk.size) { 
                            self.create_new_page(self.page_metadata.page_number);
                        }
                        let row = RowMetaData::new(self.table_metadata.clone(), key.clone(), chunk.size,  chunk_range.clone(), page_number);
                        self.rows.insert(key.clone(), row);
                        
                        
                        self.pages.append_chunks(chunk.data, chunk_range.clone());
                        match self.pages.write(self.table_metadata.table_name.clone()) {
                            true => {
                                // commit , udpate the page metadata, return a response
                                self.save_to_json();
                                Ok("succesfully written to disk")
                                        
                            },
                            false => { 
                                // rollback , page metadata , btree state to its previous state
                                self.rows.remove(&key.clone());
                                self.page_metadata.current_size = self.page_metadata.current_size - chunk.size;
                                self.pages.delete_chunks(*chunk_range.get(0).unwrap(), *chunk_range.get(1).unwrap(), false, self.table_metadata.table_name.clone());                                    
                                self.save_to_json();
                                Err(InternalStorageError::ErrWriteToDisk("error writing to disk".to_string()))
                            },
                        }
                           
                        
                    
                    },
                    Err(err) => { 
                        self.save_to_json();
                        Err(InternalStorageError::SerializerError(err))
                    },
                }
            },
            None => {
                self.save_to_json();
                Err(InternalStorageError::ErrPrimaryKeyNotFound("primary key not found".to_string()))
            },
        }
        
        
        
        
    } 
}


#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use bigdecimal::{BigDecimal, FromPrimitive};
    use crate::{page::{self, page::Page, table::{PageData, RowMetaData}}, row::{self, StoredRow}};
    use sql_one_parser::{commands::select_condition::Condition, value::Value};

    use crate::page::table::{key_type, TableMetaData};

    use super::Storage;


    //#[test] 
    pub fn test_page_rollback() { 
        let mut bytes = Vec::new();
        for i in 1..=20 {
            bytes.push(i as u8);
        }

        let modified: Vec<u8> = bytes.iter().enumerate()
            .map(|(index , mut item)| { 
                if index >= 10 { 
                    item = &0;
                }
                *item
            }).collect();
        println!("original bytes are {:?}", bytes);
        println!("modified bytes are {:?}", modified);

    }

    //#[test]
    pub fn test_persistent_write() { 
        let table_data = TableMetaData::new("users".to_string(), "id".to_string(), key_type::Number);
        let mut storage = Storage::new(Some(table_data), "users_storage.json".to_string());
        let mut rows = HashMap::new();
        rows.insert("id".to_string(), Value::Number(BigDecimal::from(1)));
        rows.insert("name".to_string(), Value::String("raja".to_string()));
        let row = StoredRow::new(rows);
        let result = storage.write(row).unwrap();
        println!("Result is {:?}", result);
        let mut another_rows = HashMap::new();
        another_rows.insert("id".to_string(), Value::Number(BigDecimal::from(2)));
        another_rows.insert("name".to_string(), Value::String("neha".to_string()));
        let another_row = StoredRow::new(another_rows);
        let res = storage.write(another_row).unwrap();
        println!("Res is {:?}", res);
        let rows = storage.read_all();
        println!("Read result : {:#?}", rows)
    }

    //#[test]
    pub fn test_storage_serializer() { 
        let mut s = Storage::default();
        let test_row = RowMetaData::new(s.clone().table_metadata, Value::Number(BigDecimal::from_i16(1).unwrap()), 32, vec![0, 31], 1);
        s.pages = Page::new(1, vec![1,2, 3,4]);
        let mut rows = BTreeMap::new();
        rows.insert(Value::Number(BigDecimal::from_i16(1).unwrap()), test_row);
        let page_meta = PageData{
            page_number: 1,
            current_size: 32,
            max_size: 4096
        };
        s.rows = rows;
        s.page_metadata = page_meta;
        match serde_json::to_string(&s) {
            Ok(r) => println!("s : {:?}", s),
            Err(err) => println!("{:?}", err),
        }
    }

    #[test]
    pub fn test_only_read() { 
        let mut s = Storage::new(None, "users_storage.json".to_string());
        let data_rows = s.read_when(None);
        println!("read results : {:#?}", data_rows);
    }


    //#[test]
    pub fn test_read_by_primary_key() { 
        let table_data = TableMetaData::new("users".to_string(), "id".to_string(), key_type::Number);
        let mut storage = Storage::new(Some(table_data), "users_storage.json".to_string());
        println!("storge snap : {:?}", storage.clone() );
        let mut rows = HashMap::new();
        rows.insert("id".to_string(), Value::Number(BigDecimal::from(1)));
        rows.insert("name".to_string(), Value::String("raja".to_string()));
        let row = StoredRow::new(rows);
        let result = storage.write(row).unwrap();
        // println!("Result is {:?}", result);
        // let mut another_rows = HashMap::new();
        // another_rows.insert("id".to_string(), Value::Number(BigDecimal::from(2)));
        // another_rows.insert("name".to_string(), Value::String("neha".to_string()));
        // let another_row = StoredRow::new(another_rows);
        // let res = storage.write(another_row).unwrap();
        // println!("Res is {:?}", res);
        // let mut third_rows = HashMap::new();
        // third_rows.insert("id".to_string(), Value::Number(BigDecimal::from(3)));
        // third_rows.insert("name".to_string(), Value::String("rajdip".to_string()));
        // let third_row = StoredRow::new(third_rows);
        // let third_result = storage.write(third_row).unwrap();
        // println!("Res is {:?}", third_result);
        let conditions = Condition { first : "name".to_string(), second : "raja".to_string(), token :"!=".to_string()};
        let row = storage.read_when(None);
        println!("read result is {:#?}", row);
        // conditional delete
        //storage.delete(None);
        
        // delete all 
        //storage.delete(None);
        //println!("storage : {:?}", storage);
        //println!("after deleting read results are {:#?}", storage.read_when(None));
    }
}