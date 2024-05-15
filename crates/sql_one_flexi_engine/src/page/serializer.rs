use crate::row::StoredRow;

use super::error::RowSerializerError;

pub trait RowSerializer { 
    fn to_bytes(&self) -> Result<Chunk, RowSerializerError> ;
    fn from_bytes(bytes : &[u8]) -> Result<Self, RowSerializerError> where Self:Sized;
}

pub struct Chunk { 
    pub size : usize, 
    pub data : Vec<u8>
}


impl RowSerializer for StoredRow {
    fn to_bytes(&self) -> Result<Chunk, RowSerializerError> {
        
        match serde_json::to_vec(&self).map_err(|err| err.to_string()) {
            Ok(buffer) => Ok(Chunk { size : buffer.len(), data : buffer}),
            Err(err) => Err(RowSerializerError::ErrRowSerialize(err)),
        } 
    }

    fn from_bytes(bytes : &[u8]) -> Result<Self, RowSerializerError>  {
        match serde_json::from_slice::<Self>(bytes).map_err(|err| err.to_string()) {
            Ok(value) => Ok(value),
            Err(err) => Err(RowSerializerError::ErrRowDeserialize(err)),
        }
        
    }
}