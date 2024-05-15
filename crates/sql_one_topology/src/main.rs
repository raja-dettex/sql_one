use std::{collections::HashMap, fmt::format, hash::Hash, net::SocketAddr, ops::Deref, str::FromStr, sync::{Arc, Mutex}, thread::{self, AccessError}, time::Duration};
use sql_one_topology::{parser::Parser, rpc::{self, RPC}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpSocket, TcpStream}};
use tokio::sync::mpsc;
use std::collections::hash_map::Iter;
use std::net::TcpStream as ClientStream;
use std::io::Write;


#[derive(Clone, Debug)]
pub struct Peer { 
    id: String,
    address : String,
    master_addr : String
}


impl Peer { 
    pub fn new(id: String, address : String, master_addr : String) -> Self {
        
        Self { id, address, master_addr }
    }

    pub async fn listen_and_accept(addr: String) { 
        let listener = TcpListener::bind(addr.clone()).await.unwrap();
        loop { 
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buff = [0; 1024];
            while let Ok(n) = s.read(&mut buff).await { 
                if n == 0  {
                    break;
                }
                println!("peer with addr {} received message {}", addr, String::from_utf8_lossy(&buff[..n]))
            }
        }
        
    }

    pub async fn handle_connection(&self, mut stream : TcpStream) { 
        let mut buff = [0 as u8;1024];
        while let Ok(n) = stream.read(&mut buff).await { 
            if n == 0 { 
                break;
            }
            let msg = String::from_utf8_lossy(&buff[..n]);
            println!("received messge from peer id : {} and msg is : {}", self.id, msg);
        }
        let addr = self.address.clone();
        tokio::spawn(async move { 
            Peer::listen_and_accept(addr).await;
        });
        
    }
    pub fn get_addr(&self) -> String { 
        self.address.clone()
    }

    // pub async fn send_messsage(&self , msg : String) { 
    //     let mut s = Arc::try_unwrap(self.stream.clone()).unwrap();
    //     s.write_all(msg.as_bytes()).await;
    // }

}


#[derive(Clone)]
pub struct P2Pnetwork {
    listen_addr : SocketAddr,
    peers : Arc<Mutex<HashMap<String, Peer>>>,
    is_master : bool
}




impl P2Pnetwork { 

    pub fn new(listen_addr : SocketAddr, is_master : bool) -> Self  { 
        Self { listen_addr  , peers : Arc::new(Mutex::new(HashMap::new())), is_master}
    }


    pub fn add_peer(&mut self, peer : Peer) { 
        self.peers.lock().unwrap().insert(peer.clone().id, peer);
    }

    pub async fn handleStream(&mut self, mut stream : TcpStream ) { 
        let mut buff = [0 as u8; 1024];
        while let Ok(n) = stream.read(&mut buff).await { 
            if n == 0 { 
                break;
            }
            let rpc = RPC::from_bytes(&buff[..n]);
            match rpc.header {
                sql_one_topology::rpc::RPCHeader::PeerAdded => {
                    let peer = Peer::new(rpc.payload.clone(), rpc.payload.clone(), self.listen_addr.clone().to_string());
                    self.add_peer(peer);
                    println!("peer list : {:#?}", self.peers);
                },
                sql_one_topology::rpc::RPCHeader::TableCreated => todo!(),
                sql_one_topology::rpc::RPCHeader::TableInserted => todo!(),
                sql_one_topology::rpc::RPCHeader::TableDeleted => todo!(),
                sql_one_topology::rpc::RPCHeader::Other => { 
                    println!("other rpc is {:#?}", rpc);
                }
            }
        }
    }


    pub async fn listen(&mut self) -> Result<(), Box<dyn std::error::Error>> { 
        let listener = TcpListener::bind(self.listen_addr.clone()).await?;
        loop { 
            match listener.accept().await {
                Ok((mut stream, addr)) => {
                    println!("addres is {:#?}", addr );
                    self.handleStream(stream).await;
                    // let peer = Peer::new(addr.clone().to_string(), addr.clone().to_string(), self.listen_addr.to_string()); 
                    // println!("peer connected is {:#?}", peer.clone());
                    // self.add_peer(peer.clone());
                    // tokio::spawn(async move { 
                    //     peer.handle_connection(stream).await;
                    // });
                    //thread::sleep(Duration::from_secs(5));
                    //sprintln!("invoking broadcast");
                    //self.broadcast(format!("peer with addr : {} has joined", addr.clone())).await;
                    let peers_clone = self.clone().peers.clone();
                    tokio::spawn( async move { 
                        P2Pnetwork::broadcast(peers_clone, "hello".to_string()).await;
                    });
                },
                Err(err) => println!("error is {:#?}", err.to_string()),
                }
            
        }
        Ok(())
    }

    pub async fn broadcast(peers : Arc<Mutex<HashMap<String, Peer>>>, msg : String) 
    { 
        for (_, peer) in peers.lock().unwrap().iter() { 
            let mut socket = ClientStream::connect(peer.address.clone()).unwrap();
            let message = RPC::new(rpc::RPCHeader::Other, msg.clone());
            let bytes = message.to_bytes();
            socket.write_all(&bytes);
        }
        
    }
}


#[tokio::main]
async fn main() {
    let addr : SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let mut master = P2Pnetwork::new(addr.clone(), true);
    //let mut network_clone = master.clone();
    tokio::spawn(async move { 
        master.listen().await.unwrap();
    });

    
    let mut slave1 = P2Pnetwork::new("127.0.0.1:8081".parse().unwrap(), false);
    //let mut network_clone = master.clone();
    tokio::spawn(async move { 
        slave1.listen().await.unwrap();
    });

    
    let mut slave2 = P2Pnetwork::new("127.0.0.1:8082".parse().unwrap(), false);
    //let mut network_clone = master.clone();
    tokio::spawn(async move { 
        slave2.listen().await.unwrap();
    });

    
    let socket = TcpSocket::new_v4().unwrap();
    
    
    // let socket_addr = SocketAddr::from_str(&addr).unwrap();
    let res = socket.connect(addr).await;
    match res { 
        Ok(mut s) => { 
            let str = "127.0.0.1:8081";
            let rpc = RPC::new(rpc::RPCHeader::PeerAdded, "127.0.0.1:8081".to_string()); 
            let bytes = rpc.to_bytes();
            s.write_all(&bytes).await;
        },
        Err(err) => { 
            println!("error is {:#?}", err);
        }
    }
    let another_socket = TcpSocket::new_v4().unwrap();
    let another_res = another_socket.connect(addr).await;
    match another_res { 
        Ok(mut s) => { 
            let str = "127.0.0.1:8081";
            let rpc = RPC::new(rpc::RPCHeader::PeerAdded, "127.0.0.1:8082".to_string()); 
            let bytes = rpc.to_bytes();
            s.write_all(&bytes).await;
        },
        Err(err) => { 
            println!("error is {:#?}", err);
        }
    }
    
    tokio::signal::ctrl_c().await.unwrap();
}
