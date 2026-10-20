use std::collections::HashMap;
use std::fmt::Display;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::result;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

type Result<T> = result::Result<T, ()>;

const SAFE_MODE: bool = false;
struct Sensitive<T>(T);

impl<T: Display> Display for Sensitive<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(t) = self;
        if SAFE_MODE {
            writeln!(f, "[REDACTED]")
        } else {
            writeln!(f, "{t}")
        }
    }
}

enum Message {
    // From the client perspective, we need to tell the server who we are.
    ClientConnected(Arc<TcpStream>),
    ClientDisconnected(Arc<TcpStream>),
    NewMessage(Arc<TcpStream>, Vec<u8>),
}

struct Client {
    conn: Arc<TcpStream>,
}

fn server(message: Receiver<Message>) -> Result<()> {
    let mut clients: HashMap<SocketAddr, Client> = HashMap::new();

    loop {
        println!("Number of clients: {}", clients.len());

        // blocking
        if let Some(message) = message.iter().next() {
            match message {
                Message::ClientConnected(stream) => {
                    let addr = stream.peer_addr().unwrap();
                    println!("New client connected: {addr}");
                    clients.insert(
                        addr,
                        Client {
                            conn: stream.clone(),
                        },
                    );
                }
                Message::ClientDisconnected(stream) => {
                    let addr = stream.peer_addr().unwrap();
                    if let Some(client) = clients.remove(&addr) {
                        println!(
                            "Client connection droppped: {}",
                            client.conn.peer_addr().unwrap()
                        );
                    }
                }
                Message::NewMessage(stream, bytes) => {
                    let author_addr = stream.peer_addr().unwrap();
                    // populate the message to all clients;
                    for (addr, client) in clients.iter() {
                        if *addr != author_addr {
                            let _ = client.conn.as_ref().write(&bytes);
                        }
                    }
                }
            }
        }
    }
}

fn process(stream: Arc<TcpStream>, messages: Sender<Message>) -> Result<()> {
    messages
        .send(Message::ClientConnected(stream.clone()))
        .map_err(|err| eprintln!("ERROR: could not send message to the server thread: {err}"))?;

    let mut buf = Vec::new();
    buf.resize(64, 0);

    loop {
        let n = stream.as_ref().read(&mut buf).map_err(|err| {
            eprintln!("ERROR: could not read message from stream: {err}");
            let _ = messages.send(Message::ClientDisconnected(stream.clone()));
            return;
        })?;
        messages
            .send(Message::NewMessage(stream.clone(), buf[0..n].to_vec()))
            .map_err(|err| {
                eprintln!("ERROR: could not send message to the server thread: {err}");
            })?;
    }
}

fn main() -> Result<()> {
    let address = "127.0.0.1:8080";
    let listener = TcpListener::bind(address).map_err(|err| {
        eprintln!(
            "Failed to start server at {}: {}",
            Sensitive(address),
            Sensitive(err),
        );
    })?;
    let (tx, rx) = channel::<Message>();
    thread::spawn(|| server(rx));

    println!("INFO: listening to {}", Sensitive(address));

    for stream in listener.incoming() {
        match stream {
            Ok(stream_handle) => {
                let tx = tx.clone();
                let stream = Arc::new(stream_handle);
                thread::spawn(|| process(stream, tx).unwrap());
            }
            Err(err) => eprintln!("Failed to take stream: {err}"),
        }
    }
    Ok(())
}
