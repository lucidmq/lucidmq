use log::{error, info, debug};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::io;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;

use crate::msgpack_helper::parse_request;
use crate::types::Command;

use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};

use crate::lucidmq_errors::ServerError;
use crate::types::{RecieverType, SenderType};

type PeerMap = Arc<Mutex<HashMap<String, OwnedWriteHalf>>>;

pub struct LucidTcpServer {
    peer_map: PeerMap,
    address: SocketAddr,
    sender: SenderType,
    reciever: RecieverType,
}

impl LucidTcpServer {
    /// Initialize a new instance of the TCP server. Takes in a host and port to listen in on and sender and reciever channels to comunicate with the broker.
    pub fn new(
        host: &str,
        port: &str,
        sender: SenderType,
        reciever: RecieverType,
    ) -> Result<LucidTcpServer, ServerError> {
        let addr_string = format!("{}:{}", host, port);
        let addr = addr_string.parse().map_err(|e| {
            error!("{}", e);
            ServerError::new("Unable to parse host string and port into socketaddress")
        })?;
        Ok(LucidTcpServer {
            peer_map: PeerMap::new(Mutex::new(HashMap::new())),
            address: addr,
            sender: sender,
            reciever: reciever,
        })
    }

    /// Runs a tcp server bound to given address.
    pub async fn run_server(self) {
        info!("Server Listening on {}", self.address.to_string());
        let listener = TcpListener::bind(self.address).await.unwrap();

        let arc_peer_map = Arc::new(self.peer_map.clone());
        tokio::spawn(async move {
            handle_responses(self.reciever, arc_peer_map).await;
        });

        loop {
            let (stream, _) = listener.accept().await.unwrap();
            info!("connection accepted: addr={}", stream.peer_addr().unwrap());
            let cloned_sender = self.sender.clone();
            let arc_peer_map = Arc::new(self.peer_map.clone());
            tokio::spawn(async move {
                handle_connection(stream, arc_peer_map, cloned_sender).await;
            });
        }
    }
}

/// Every incoming TCP connection create a connection string and adds an entry to the connection map(peermap), and then proceeds to handle the request.
///  Once the connection is terminated, the connection entry in the map is removed
async fn handle_connection(stream: TcpStream, peermap: Arc<PeerMap>, sender: SenderType) {
    let id: String = generate_connection_string();
    let (rx, tx) = stream.into_split();
    peermap.lock().await.insert(id.clone(), tx);
    handle_request(id.clone(), rx, sender).await;
    peermap.lock().await.remove(&id);
    info!("Connection for {} terminatied", &id);
}

/// Handle request listens in on the open TCP stream. It has some logic for translating incoming bytes to message types.
/// These are then sent along to the broker via the sender channel.
async fn handle_request(conn_id: String, recv: OwnedReadHalf, sender: SenderType) {
    let mut buf;
    loop {
        buf = [0u8; 2];
        recv.readable().await.unwrap_or_else(|err| {
            error!("TCP stream not readable: {}", err);
        });
        let bytes_read = recv.try_read(&mut buf);
        let message_size: u16 = match bytes_read {
            Ok(0) => break,
            Ok(_total) => {
                let message_size = u16::from_le_bytes(buf);
                message_size
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(err) => {
                error!("Unable to read first bytes in stream: {}", err);
                break;
            }
        };
        let mut message_vec = vec![0u8; message_size.into()];
        let message_buff = &mut message_vec;

        let message_bytes_read = recv.try_read(message_buff);
        match message_bytes_read {
            Ok(total) => {
                debug!("Second Bytes recieved {:?} size {}", message_buff, total);
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(err) => {
                error!("Unable to read first bytes in stream: {}", err);
                break;
            }
        };
        
        let command = parse_request(conn_id.clone(), message_buff.clone());
        sender.send(command).await.expect("Unble to send message");
    }
}

/// Handles incoming responses from the Reciever channel(sent from the broker). 
/// That message is then matched to a stream in the peermap, where the message data will be sent to.
async fn handle_responses(mut reciever: RecieverType, peermap: Arc<PeerMap>) {
    while let Some(command) = reciever.recv().await {
        let id;
        let response_message: Vec<u8>;
        match command {
            Command::Response {
                conn_id,
                capmessagedata,
            } => {
                id = conn_id;
                response_message = capmessagedata;
            }
            Command::Invalid {
                conn_id,
                error_message: _,
                capmessage_data,
            } => {
                id = conn_id;
                response_message = capmessage_data;
            }
            _ => {
                error!("Command not good");
                continue;
            }
        }
        let wing = peermap.lock().await;

        match wing.get(&id) {
            Some(outgoing) => {
                outgoing
                    .try_write(&response_message)
                    .unwrap_or_else(|error| {
                        error!("Unable to write to tcp stream: {:?}", error);
                        0
                    });
            }
            None => {
                error!("Unable to find connection key: {}", &id);
            }
        }
    }
}

/// Generates a new random connection string.
fn generate_connection_string() -> String {
    let rand_string: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    return rand_string;
}