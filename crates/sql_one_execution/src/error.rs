use miette::Diagnostic;
use derive_more::Display;
use sql_one_parser::{commands::create::SqlTypeInfo, error::FormattedError, value::Value};
use thiserror::Error;



#[derive(Debug, Diagnostic, Error)]
#[error("query execution error")]
pub enum QueryExecutionError { 
    #[error("table {0} was not found")]
    TableNotFound(String),
    #[error("table {0} already exists ")]
    TableAlreadyExists(String),
    #[error("column {0} was not found")]
    ColumnNotFound(String),
    #[error("Value {1} can not be inserted into a {0} column")]
    InsertTypeMismatch(SqlTypeInfo, Value),
}


#[derive(Debug, Diagnostic, Error)]
#[error(transparent)]
pub enum SQLError<'a> {

    #[diagnostic(transparent)] 
    QueryExecutionError(#[from] QueryExecutionError),

    #[diagnostic(transparent)]
    ParsingError(FormattedError<'a>)
}

impl<'a> From<FormattedError<'a>> for SQLError<'a> {
    fn from(value: FormattedError<'a>) -> Self {
        SQLError::ParsingError(value)
    }
}