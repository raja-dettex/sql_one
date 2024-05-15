
#[derive(Debug)]
pub enum RowSerializerError { 
    ErrRowSerialize(String),
    ErrRowDeserialize(String)
}

#[derive(Debug)]
pub enum InternalStorageError { 
    ErrPrimaryKeyNotFound(String),
    ErrWriteToDisk(String),
    ErrReadFromDisk(String),
    ErrInternal(String),
    SerializerError(RowSerializerError)
}