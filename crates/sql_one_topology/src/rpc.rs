use serde::{Deserialize, Serialize};

use crate::parser::Parser;


#[derive(Debug, Serialize, Deserialize)]
pub struct RPC { 
    pub header:  RPCHeader,
    pub payload : String,
}

impl Parser for RPC {
    fn from_bytes(bytes : &[u8]) -> Self {
        let rpc : RPC = serde_json::from_slice(bytes).unwrap();
        rpc
    }

    fn to_bytes(self) -> Vec<u8> {
        let bytes = serde_json::to_vec(&self).unwrap();
        bytes
    }
}

impl RPC { 
    pub fn new(header : RPCHeader, payload : String) -> Self { 
        Self { header, payload}
    }
}


#[derive(Serialize, Deserialize, Debug )]
pub enum RPCHeader { 
    PeerAdded,
    TableCreated,
    TableInserted,
    TableDeleted,
    Other
    
}