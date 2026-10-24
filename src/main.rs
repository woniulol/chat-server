use std::collections::HashMap;
use std::fmt::Display;
use std::io::{Read, Write};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::ops::Deref;
use std::result;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::{Duration, SystemTime};

type Result<T> = result::Result<T, ()>;

const SAFE_MODE: bool = false;
const BANNED_LIMIT: Duration = Duration::from_secs(10 * 60);
const MESSAGE_RATE: Duration = Duration::from_secs(1);
const STRICK_LIMIT: i32 = 3;

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
    //
    // TcpStream from ClientConnected will be registered so that ClientDisconnected and NewMessage
    // only requires SocketAddr.
    ClientConnected(Arc<TcpStream>),
    ClientDisconnected(SocketAddr),
    NewMessage(SocketAddr, Vec<u8>),
}

struct Client {
    conn: Arc<TcpStream>,
    last_message: SystemTime,
    strick_count: i32,
}

fn server(message: Receiver<Message>) -> Result<()> {
    let mut allowed_clients: HashMap<SocketAddr, Client> = HashMap::new();
    let mut banned_addrs: HashMap<IpAddr, SystemTime> = HashMap::new();

    loop {
        println!("Number of clients: {}", allowed_clients.len());

        // blocking
        if let Some(message) = message.iter().next() {
            match message {
                Message::ClientConnected(stream) => {
                    let addr = stream.peer_addr().unwrap();

                    if let Some(banned_at) = banned_addrs.get(&addr.ip()) {
                        let now = SystemTime::now();
                        let banned_duration = now
                            .duration_since(*banned_at)
                            .expect("TODO: clock might go backwards");

                        if banned_duration < BANNED_LIMIT {
                            let msg = format!(
                                "You are banned: {} secs left",
                                (BANNED_LIMIT - banned_duration).as_secs()
                            );
                            let _ = stream.deref().write(&msg.as_bytes());
                            let _ = stream.shutdown(Shutdown::Both);

                            continue;
                        } else {
                            println!("Banned client released: {addr}");
                            banned_addrs.remove(&addr.ip());
                        }
                    }

                    println!("New client connected: {addr}");
                    allowed_clients.insert(
                        addr,
                        Client {
                            conn: stream.clone(),
                            last_message: SystemTime::now(),
                            strick_count: 0,
                        },
                    );
                }

                Message::ClientDisconnected(socket_addr) => {
                    if let Some(client) = allowed_clients.remove(&socket_addr) {
                        println!(
                            "Client connection droppped: {}",
                            client.conn.peer_addr().unwrap()
                        );
                    }
                }

                Message::NewMessage(socket_addr, bytes) => {
                    if let Some(client) = allowed_clients.get_mut(&socket_addr) {
                        let now = SystemTime::now();

                        if now.duration_since(client.last_message).unwrap() > MESSAGE_RATE {
                            client.last_message = now;

                            // check is valid utf8
                            if str::from_utf8(&bytes).is_ok() {
                                // populate the message to all clients;
                                for (addr, client) in allowed_clients.iter() {
                                    if *addr != socket_addr {
                                        let _ = client.conn.as_ref().write(&bytes);
                                    }
                                }
                            } else {
                                client.strick_count += 1;
                                if client.strick_count > STRICK_LIMIT {
                                    let msg = format!(
                                        "You are banned: {} secs left",
                                        BANNED_LIMIT.as_secs()
                                    );
                                    let _ = client.conn.as_ref().write(&msg.as_bytes());
                                    let _ = client.conn.shutdown(Shutdown::Both);
                                    banned_addrs.insert(client.conn.peer_addr().unwrap().ip(), now);
                                }
                            }
                        } else {
                            client.strick_count += 1;
                            if client.strick_count > STRICK_LIMIT {
                                let msg =
                                    format!("You are banned: {} secs left", BANNED_LIMIT.as_secs());
                                let _ = client.conn.as_ref().write(&msg.as_bytes());
                                let _ = client.conn.shutdown(Shutdown::Both);
                                banned_addrs.insert(client.conn.peer_addr().unwrap().ip(), now);
                            }
                        }
                    };
                }
            }
        }
    }
}

fn process(stream: Arc<TcpStream>, messages: Sender<Message>) -> Result<()> {
    let socket_addr = stream
        .peer_addr()
        .map_err(|err| eprint!("ERROR: could not get socker address: {err}"))?;

    messages
        .send(Message::ClientConnected(stream.clone()))
        .map_err(|err| eprintln!("ERROR: could not send message to the server thread: {err}"))?;

    let mut buf = Vec::new();
    buf.resize(64, 0);

    loop {
        let n = stream.as_ref().read(&mut buf).map_err(|err| {
            eprintln!("ERROR: could not read message from stream: {err}");
            let _ = messages.send(Message::ClientDisconnected(socket_addr));
            return;
        })?;

        if n == 0 {
            let _ = messages.send(Message::ClientDisconnected(socket_addr));
            return Ok(());
        }

        messages
            .send(Message::NewMessage(socket_addr, buf[0..n].to_vec()))
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
                thread::spawn(|| {
                    let _ = process(stream, tx);
                });
            }
            Err(err) => eprintln!("Failed to take stream: {err}"),
        }
    }
    Ok(())
}
