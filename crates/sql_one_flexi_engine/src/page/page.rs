use std::{fs::{self, File, OpenOptions}, io::{Read, Write}};

use serde::{Deserialize, Serialize};


pub const PAGE_SIZE : usize = 4096;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page { 
    pub page_number : usize,
    pub data : Vec<u8>
}


impl Page { 
    pub fn default(page_number : usize) -> Self {
        Self { 
            page_number,
            data : Vec::new()
        }
    }

    pub fn new(page_number : usize, data : Vec<u8>) -> Self {
        Self { 
            page_number,
            data
        }
    }   

    pub fn append_chunks(&mut self, mut chunks : Vec<u8>, range : Vec<usize>)  { 
        self.data.append(&mut chunks);
    }

    pub fn delete_chunks(&mut self , start : usize , end : usize, is_disk_cleanup : bool, table_name : String) -> Page{
        let mut updated_data = Vec::new();
        for (index , item) in self.data.iter().enumerate() {
            if index >= start && index <= end { 
                continue
            }
            updated_data.push(*item);
        }
        self.data = updated_data;
        if is_disk_cleanup { 
            self.write(table_name);
        }
        self.clone()
    }

    pub fn read(page_number : usize, table_name : String) -> Option<Self> { 
        let mut file = match File::open(format!("storage/{}/page_{}.bin", table_name, page_number)) {
            Ok(file) => file,
            Err(err) => {
                println!("here error opening the file {}", err);
                return None;
            },
        };
        let mut buffer = vec![0; PAGE_SIZE];
        match file.read(&mut buffer) {
            Ok(_) => Some(Self{page_number: page_number, data : buffer}),
            Err(err) => {
                println!("error reading the file {}", err);
                None
            },
        }
    }

    pub fn read_chunks(page_number : usize, chunk_range: Vec<usize>, table_name: String) -> Option<Vec<u8>>{
    

        let mut file = match File::open(format!("storage/{}/page_{}.bin", table_name, page_number)) {
            Ok(file) => file,
            Err(err) => {
                println!("here error opening the file {}", err);
                return None;
            },
        };
        let mut buffer = vec![0; PAGE_SIZE];
        match file.read(&mut buffer) {
            Ok(_) => { 
                let data: Vec<u8> = buffer.iter().enumerate().filter(|(index , item)| index >= chunk_range.get(0).unwrap() && index <= chunk_range.get(1).unwrap())
                    .map(|(index, item)| *item)
                    .collect();
                Some(data)
            },
            Err(err) => {
                println!("error reading the file {}", err);
                None
            },
        }
    }

    pub fn delete(page_number : usize, table_name : String) -> Result<(), String>{
        fs::remove_file(format!("storage/{}/page_{}.bin", table_name, page_number)).map_err(|err| err.to_string())
    }



    pub fn write(&self, table_name : String) -> bool { 

        let current_dir = match std::env::current_dir() {
            Ok(dir) => dir,
            Err(err) => {
                eprintln!("Error getting current directory: {}", err);
                return false;
            }
        };
        let storage_dir = current_dir.join("storage");
        if let Err(err) = fs::create_dir_all(&storage_dir) {
            eprintln!("Error creating storage directory: {}", err);
            return false;
        }

        let table_dir = storage_dir.join(table_name);
        if let Err(err) = fs::create_dir_all(&table_dir) {
            eprintln!("Error creating table directory: {}", err);
            return false;
        }

        let file_path = table_dir.join(format!("page_{}.bin", self.page_number));
        let mut file = match OpenOptions::new()
            .write(true)
            .create(true)
            .open(&file_path) 
        {
            Ok(file) => file ,
            Err(err) => {
                println!("error opening the file : {}", err);
                return false;
            }
        };
        match file.write_all(&self.data) {
            Ok(_) => true,
            Err(err) => {
                println!("error writing to file : {}", err);
                false
            }
        }
    }
}



#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;
    use bigdecimal::{BigDecimal, FromPrimitive};
    use sql_one_parser::value::Value;
    use crate::{page::{serializer::RowSerializer, table::{key_type, PageData, RowMetaData, TableMetaData}}, row::StoredRow}; 

    //#[test]
    pub fn test_writeandRead()  {
        // create page metadata and table metadata
        let mut page_data = PageData::default(1);
        let table_data = TableMetaData::new("users".to_string(), "id".to_string(), key_type::Number);
        let mut row_data_vec = Vec::new();
        // pretest setup 
        // cleanup the disk data
        let result = Page::delete(1, "users".to_string());
        if result.is_err() {
            panic!("clean up stuck");
        }
        // create a page with data
        // 1 create a stored row containing columns and values
        let mut rows = HashMap::new();
        rows.insert("id".to_string(), Value::Number(BigDecimal::from(1)));
        rows.insert("name".to_string(), Value::String("raja".to_string()));
        let row = StoredRow::new(rows);

        // 2 serialize into vec<u8>

        let data = row.to_bytes().unwrap();
        println!("data is {:?}", data.data);
        let (page_number , range) = page_data.getChunkData(data.size); 
        let row1 = RowMetaData::new(table_data, Value::Number(BigDecimal::from_i16(1).unwrap()), data.size, range, page_number);
        row_data_vec.push(row1.clone());
        
        //println!("data is {:?}", data);
        let page = Page::new(1, data.data);
        println!("{:?}", page);
        
        // write it to disk
        let result = page.write("users".to_string());
        println!("{}", result);

        // check the assertion
        assert!(result);
        let page = Page::read_chunks(1, row1.range.clone(),  "users".to_string());
        assert_eq!(page.is_none(), false);
        if let Some(data) = page {
            println!("page is {:?}", data);
            let row = StoredRow::from_bytes(&data).unwrap();
            println!("row is {:#?}", row);
        }        
    }


    //#[test]
    pub fn test_read() { 
        let page = Page::read(1, "users".to_string());
        assert_eq!(page.is_none(), false);
        if let Some(data) = page {
            println!("page is {:?}", data);
            let row = StoredRow::from_bytes(&data.data).unwrap();
            println!("row is {:#?}", row);
        }
    }


    //#[test]
    pub fn serialize_deserialize() { 
        let mut rows = HashMap::new();
        rows.insert("id".to_string(), Value::Number(BigDecimal::from(1)));
        rows.insert("name".to_string(), Value::String("raja".to_string()));
        let row = StoredRow::new(rows);
        let bytes = row.to_bytes().unwrap();
        println!("bytes are {:?}",bytes.data);
        let retrieved = StoredRow::from_bytes(&bytes.data).unwrap();
        println!("retrieved is {:#?}", retrieved);
        assert_eq!(retrieved, row);
        
    }
}