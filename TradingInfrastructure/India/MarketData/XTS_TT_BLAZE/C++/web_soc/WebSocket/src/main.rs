use std::sync::{Arc, Mutex};
use std::thread;
use std::io::{self, Write, Read};
use std::net::{TcpStream, ToSocketAddrs};


trait WebSocketClient {
    fn connect(&mut self);
    fn handle_event(&self, event: &str, data: &str);
    fn start(&self);
}

struct Client {
    server_addr: String,
    token: String,
    user_id: String,
    publish_format: String,
    broadcast_mode: String,
    connected: Arc<Mutex<bool>>,
}

impl Client {
    fn new(server_addr: String, token: String,
        user_id: String, publish_format: String,
        broadcast_mode: String) -> Self {
        
                Client {
                    server_addr,
                    token,
                    user_id,
                    publish_format,
                    broadcast_mode,
                    connected: Arc::new(Mutex::new(false)),
                }

            }
    
    fn build_url(&self) -> String {
        format!("https://ttblaze.iifl.com/?token={}&userID={}&publishFormat={}&broadcastMode={}",
                self.token, self.user_id, self.publish_format, self.broadcast_mode)
    }

    fn handle_data_event(&self, event_type: &str, data: &str){
        println!("Handling event {}: Data - {}",event_type, data);
    }
}

impl WebSocketClient for Client {
    fn connect(&mut self) {
        let base_url = self.base_url();
        
        match TcpStream::connect(addr) {
            Ok(mut stream) => {
                *self.connected.lock().unwrap() = true;

                println!("Connected to the server at {}", base_url);

                let connected_handle = Arc::clone(&self.connected);
                
                thread::spawn(move || {
                    let mut buffer = [0; 1024];
                    while *self.connected.lock().unwrap(){
                        match stream.read(&mut buffer){
                            Ok(bytes_read) => {
                                if bytes_read > 0 {
                                    let data = String::from_utf8_lossy(&buffer[..bytes_read]);
                                    println!("Recieved: {}", data);
                                }
                            }
                            Err(e) => {
                                println!("Error reading from server: {}",e);
                                *connected_handle.lock().unwrap() = false;
                            }
                        }

                    }
                });
                Err(e) => {
                    println!("Failed to connect to the server: {}",e);
                } 

            }
        }
    }

    
}