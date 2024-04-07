use std::{
    collections::HashMap,
    net::{IpAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, Error, Result},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

const CRLF: &str = "\r\n";
const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args();
    let mut port = 6379;
    let mut mode = Mode::Master;
    let mut urandom = File::open("/dev/urandom")
        .await
        .expect("failed to open /dev/urandom");
    let mut buffer = [0u8; 20]; // 40 random bytes desired; hex encoding will double the size of the input
    urandom.read_exact(&mut buffer).await?;
    let replid = hex::encode(buffer);
    while let Some(arg) = args.next() {
        println!("arg: {arg}");
        match arg.as_str() {
            "--help" => {
                println!("Usage: redis-server [options]");
                println!("Options:");
                println!("  --help: display this help message");
                println!("  --port <PORT>: specify the port to listen on (default: 6379)");
                println!("  --replicaof <MASTER_HOST> <MASTER_PORT>: start in replica mode, connecting to the specified master");
                std::process::exit(0);
            }
            "--port" => {
                port = args
                    .next()
                    .expect("missing port argument")
                    .parse::<u16>()
                    .expect("invalid port argument");
                println!("port: {port}");
            }
            "--replicaof" => {
                let master_host = args.next().expect("missing master host argument");
                let master_port = args
                    .next()
                    .expect("missing master port argument")
                    .parse::<u16>()
                    .expect("invalid master port argument");
                let addr = format!("{master_host}:{master_port}")
                    .to_socket_addrs()
                    .unwrap_or_else(|e| {
                        eprintln!("invalid master host argument: {master_host} ({e})");
                        std::process::exit(1);
                    })
                    .next()
                    .unwrap_or_else(|| {
                        eprintln!("invalid master host argument: {master_host}");
                        std::process::exit(1);
                    });
                println!("replicaof: {addr:?}");
                mode = Mode::Replica(addr.ip(), addr.port());
            }
            _ => {
                eprintln!("ignoring unknown argument: {arg}");
            }
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .expect("failed to bind TCP");
    println!(
        "listening on {}",
        listener.local_addr().expect("failed to read local address")
    );

    if let Mode::Replica(ip, master_port) = mode {
        let mut master = TcpStream::connect((ip, master_port))
            .await
            .expect("failed to connect to master");
        let ping_handshake = format!("*1{CRLF}$4{CRLF}ping{CRLF}");
        master
            .write_all(ping_handshake.as_bytes())
            .await
            .expect("failed initiate handshake with master");
        master
            .flush()
            .await
            .expect("failed to flush master connection after ping");
        let mut buf = vec![0; 512];
        let len = master
            .read(&mut buf)
            .await
            .expect("failed to read ping handshake response from master");
        let resp = std::str::from_utf8(&buf[..len]).expect("invalid utf8 string");
        if let Ok((resp, _)) = decode_resp(resp) {
            match resp {
                DataType::SimpleString(ref s) => {
                    if s.to_lowercase().as_str() != "pong" {
                        eprintln!("unexpected response from master: {s}");
                        std::process::exit(1);
                    }
                }
                _ => {
                    eprintln!("unexpected handshake response from master: {resp:?}");
                    std::process::exit(1);
                }
            }
        }
        let replconf_port = format!(
            "*3{CRLF}$8{CRLF}REPLCONF{CRLF}$14{CRLF}listening-port{CRLF}${port_len}{CRLF}{port}{CRLF}",
            port_len = port.to_string().len(),
        );
        master
            .write_all(replconf_port.as_bytes())
            .await
            .expect("failed to send REPLCONF listening-port to master");
        master
            .flush()
            .await
            .expect("failed to flush master connection after replconf listening-port");

        let mut buf = vec![0; 512];
        let len = master
            .read(&mut buf)
            .await
            .expect("failed to read replconf listening-port response from master");
        let resp = std::str::from_utf8(&buf[..len]).expect("invalid utf8 string");
        if let Ok((resp, _)) = decode_resp(resp) {
            match resp {
                DataType::SimpleString(ref s) => {
                    if s.to_lowercase().as_str() != "ok" {
                        eprintln!("unexpected response from master: {s}");
                        std::process::exit(1);
                    }
                }
                _ => {
                    eprintln!("unexpected replconf listening-port response from master: {resp:?}");
                    std::process::exit(1);
                }
            }
        } else {
            eprintln!("failed to decode replconf listening-port response from master");
            std::process::exit(1);
        }

        let replconf_capa =
            format!("*3{CRLF}$8{CRLF}REPLCONF{CRLF}$4{CRLF}capa{CRLF}$6{CRLF}psync2{CRLF}");
        master
            .write_all(replconf_capa.as_bytes())
            .await
            .expect("failed to send REPLCONF capa to master");
        master
            .flush()
            .await
            .expect("failed to flush master connection after replconf capa");

        let mut buf = vec![0; 512];
        let len = master
            .read(&mut buf)
            .await
            .expect("failed to read replconf capa response from master");
        let resp = std::str::from_utf8(&buf[..len]).expect("invalid utf8 string");
        if let Ok((resp, _)) = decode_resp(resp) {
            match resp {
                DataType::SimpleString(ref s) => {
                    if s.to_lowercase().as_str() != "ok" {
                        eprintln!("unexpected response from master: {s}");
                        std::process::exit(1);
                    }
                }
                _ => {
                    eprintln!("unexpected replconf capa response from master: {resp:?}");
                    std::process::exit(1);
                }
            }
        } else {
            eprintln!("failed to decode replconf capa response from master");
            std::process::exit(1);
        }

        let psync = format!("*3{CRLF}$5{CRLF}PSYNC{CRLF}$1{CRLF}?{CRLF}$2{CRLF}-1{CRLF}");
        master
            .write_all(psync.as_bytes())
            .await
            .expect("failed to send PSYNC to master");
        master
            .flush()
            .await
            .expect("failed to flush master connection after psync");

        let mut buf = vec![0; 4096];
        let len = master
            .read(&mut buf)
            .await
            .expect("failed to read psync response from master");
        let resp = String::from_utf8_lossy(&buf[..len]).to_string();
        if let Ok((resp, _)) = decode_resp(resp.as_str()) {
            match resp {
                DataType::BulkString(ref s) | DataType::SimpleString(ref s) => {
                    if s.to_lowercase().is_empty() {
                        eprintln!("unexpected response from master: {s}");
                        std::process::exit(1);
                    } else {
                        // TODO: implement me!
                        println!("{s}")
                    }
                }
                _ => {
                    eprintln!("unexpected psync response from master: {resp:?}");
                    std::process::exit(1);
                }
            }
        } else {
            eprintln!("failed to decode psync response from master");
            std::process::exit(1);
        }
        println!("replication established with {ip}:{port}");
    } else {
        println!("master_replid: {replid}");
    }

    let pending_replication_clients: Arc<Mutex<HashMap<(IpAddr, u16), ReplicationClient>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let connected_replication_clients = Arc::new(Mutex::new(Vec::<ReplicationClient>::new()));
    let db: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let replid = replid.clone();
        let (stream, _addr) = listener
            .accept()
            .await
            .expect("failed to accept incoming connection");
        println!("accepted new connection");
        let db = db.clone();
        let pending_replication_clients = pending_replication_clients.clone();
        let connected_replication_clients = connected_replication_clients.clone();
        let stream = Arc::new(Mutex::new(stream));
        tokio::spawn(async move {
            loop {
                let mut buf = vec![0; 512];
                let mut guard = stream.lock().await;
                let len = guard
                    .read(&mut buf)
                    .await
                    .expect("failed to read incoming stream");
                drop(guard);
                if len == 0 {
                    break;
                }
                let input = std::str::from_utf8(&buf[..len]).expect("invalid utf8 string");
                println!("received: {:?}", input);
                let (commands, error) = match parse(input) {
                    Ok(commands) => (commands, None),
                    Err(e) => {
                        eprintln!("error parsing input: {:?}", e);
                        (vec![], Some(e.to_string()))
                    }
                };
                if commands.is_empty() {
                    let message = if let Some(error) = error {
                        format!("-ERR {error}{CRLF}")
                    } else {
                        format!("-ERR invalid request formatting{CRLF}")
                    };
                    let mut guard = stream.lock().await;
                    guard
                        .write_all(message.as_bytes())
                        .await
                        .expect("failed to write failure response to stream");
                    guard
                        .flush()
                        .await
                        .expect("failed to flush stream after failure response");
                    drop(guard);
                    break;
                }
                println!("commands: {:?}", commands);
                let mut responses = vec![];
                for command in commands.iter().cloned() {
                    println!("command: {:?}", command);
                    let response = match command {
                        Command::Ping => format!("+PONG{CRLF}"), // TODO: leverage serde, a la serde_bencode -- serde_resp?
                        Command::Del(keys) => {
                            let mut db = db.lock().await;
                            let mut count = 0;
                            for key in keys {
                                if db.remove(&key).is_some() {
                                    count += 1;
                                }
                            }
                            let connected_replication_clients =
                                connected_replication_clients.lock().await;
                            if !connected_replication_clients.is_empty() {
                                for client in connected_replication_clients.iter() {
                                    let input = String::from(input);
                                    let mut client = client.clone();
                                    tokio::task::spawn(async move {
                                        println!(
                                        "replicating del command to slave {client:?}: {input:?}",
                                        client = (client.listening_host, client.listening_port)
                                    );
                                        let mut guard = client
                                            .stream
                                            .as_mut()
                                            .expect(
                                                "missing stream for connected replication client",
                                            )
                                            .lock()
                                            .await;
                                        guard.write_all(input.as_bytes()).await.expect(
                                            "failed to write del command to replication client",
                                        );
                                        guard
                                            .flush()
                                            .await
                                            .expect("failed to flush stream after writing del command to replication client");
                                    });
                                }
                            }
                            format!(":{count}{CRLF}")
                        }
                        Command::Echo(echo_value) => {
                            let len = echo_value.len();
                            format!("${len}{CRLF}{echo_value}{CRLF}")
                        }
                        Command::Get(key) => {
                            let db = db.clone();
                            let db = db.lock().await;
                            db.get(&key)
                                .map(|val: &String| {
                                    format!("${len}{CRLF}{val}{CRLF}", len = val.len())
                                })
                                .unwrap_or(format!("$-1{CRLF}"))
                                .to_string()
                        }
                        Command::Info(_sections) => {
                            let replication_clients = connected_replication_clients.lock().await;
                            let replication_clients_count = replication_clients.len();
                            let info = format!("# Replication\nrole:{mode}\nconnected_slaves:{replication_clients_count}\nmaster_replid:{replid}\nmaster_repl_offset:0");
                            format!("${len}{CRLF}{info}{CRLF}", len = info.len())
                        }
                        Command::Psync(desired_replid, offset) => {
                            println!("got psync for replid {desired_replid} at offset {offset}");
                            let guard = stream.lock().await;
                            let socket = guard.peer_addr().expect("failed to get peer address");
                            let (host, port) = (socket.ip(), socket.port());
                            println!("client: {host}:{port} wants to psync to {desired_replid} at {offset}");
                            drop(guard);
                            match (desired_replid.as_str(), offset) {
                                ("?", -1) => {
                                    // client.state = ReplicationState::Sync;
                                    format!("+FULLRESYNC {replid} 0{CRLF}")
                                }
                                unknown => {
                                    // format!("+CONTINUE {replid} {offset}{CRLF}")
                                    unimplemented!(
                                        "psync with unexpected replid & offset {unknown:?}"
                                    );
                                }
                            }
                        }
                        Command::ReplConf(arg, val) => match (arg.as_str(), val.as_str()) {
                            ("listening-port", val) => {
                                let port = val.parse::<u16>().expect("invalid replconf port");
                                let guard = stream.lock().await;
                                let host =
                                    guard.peer_addr().expect("failed to get peer address").ip();
                                drop(guard);
                                println!("got REPLCONF listening-port {port}");
                                let client = ReplicationClient {
                                    listening_port: port,
                                    listening_host: host,
                                    state: ReplicationState::Handshake,
                                    stream: None,
                                };
                                pending_replication_clients
                                    .lock()
                                    .await
                                    .insert((host, port), client);
                                format!("+OK{CRLF}")
                            }
                            ("capa", "psync2") => {
                                let guard = stream.lock().await;
                                let socket = guard.peer_addr().expect("failed to get peer address");
                                let (host, port) = (socket.ip(), socket.port());
                                println!("client: {host}:{port} has capa psync2");
                                drop(guard);
                                format!("+OK{CRLF}")
                            }
                            unknown => {
                                unimplemented!("replconf with unexpected arg & val {unknown:?}");
                            }
                        },
                        Command::Set(key, val, expiry) => {
                            let db = db.clone();
                            let mut db_writable = db.lock().await;
                            // ACTIVE EXPIRY: queue a task to proactively delete the key after expiry ms.
                            //   * TODO: how to cancel task if key is updated before expiry?
                            // alt., PASSIVE EXPIRY: write expiry time along with key to db, and delete on GET once expiry elapsed.
                            db_writable.insert(key.clone(), val);
                            drop(db_writable);
                            if let Some(expiry) = expiry {
                                let db = db.clone();
                                tokio::task::spawn(async move {
                                    println!("queuing expiry for key: {key:?} in {expiry}ms");
                                    tokio::time::sleep(Duration::from_millis(expiry)).await;
                                    let mut db_writable = db.lock().await;
                                    db_writable.remove(&key.clone());
                                    println!("expired key: {key:?}");
                                });
                            }
                            let connected_replication_clients =
                                connected_replication_clients.lock().await;
                            if !connected_replication_clients.is_empty() {
                                for client in connected_replication_clients.iter() {
                                    let input = String::from(input);
                                    let mut client = client.clone();
                                    tokio::task::spawn(async move {
                                        println!(
                                        "replicating set command to slave {client:?}: {input:?}",
                                        client = (client.listening_host, client.listening_port)
                                    );
                                        let mut guard = client
                                            .stream
                                            .as_mut()
                                            .expect(
                                                "missing stream for connected replication client",
                                            )
                                            .lock()
                                            .await;
                                        guard.write_all(input.as_bytes()).await.expect(
                                            "failed to write set command to replication client",
                                        );
                                        guard
                                            .flush()
                                            .await
                                            .expect("failed to flush stream after writing set command to replication client");
                                    });
                                }
                            }
                            format!("+OK{CRLF}")
                        }
                    };
                    responses.push(response);
                }
                let mut guard = stream.lock().await;
                for response in responses {
                    guard
                        .write_all(response.as_bytes())
                        .await
                        .expect("failed to write success response to stream");
                    guard
                        .flush()
                        .await
                        .expect("failed to flush stream after success response");
                }

                if let Some(Command::Psync(replid, offset)) = &commands.first() {
                    let socket = guard.peer_addr().expect("failed to get peer address");
                    let (host, port) = (socket.ip(), socket.port());
                    let mut replication_clients = pending_replication_clients.lock().await;
                    match (replid.as_str(), offset) {
                        ("?", -1) => {
                            println!("handling PSYNC - needs RDB snapshot");
                            let rdb_snapshot =
                                hex::decode(EMPTY_RDB_HEX).expect("failed to decode empty rdb hex");
                            let snapshot_header = format!("${len}{CRLF}", len = rdb_snapshot.len());
                            guard
                                .write_all(snapshot_header.as_bytes())
                                .await
                                .expect("failed to write rdb snapshot header to stream");
                            guard
                                .write_all(&rdb_snapshot)
                                .await
                                .expect("failed to write rdb snapshot to stream");
                            guard
                                .flush()
                                .await
                                .expect("failed to flush stream after rdb snapshot");
                            let connected_client = ReplicationClient {
                                listening_host: host,
                                listening_port: port,
                                state: ReplicationState::Connected,
                                stream: Some(stream.clone()),
                            };
                            replication_clients.remove(&(host, port)); // this probably wont exist if these guys send from a random port
                            let mut connected_replication_clients =
                                connected_replication_clients.lock().await;
                            connected_replication_clients.push(connected_client);
                        }
                        unknown => unimplemented!("unexpected psync replid & offset: {unknown:?}"),
                    }
                    break;
                }
            }
        });
    }
}

pub(crate) fn parse(input: &str) -> Result<Vec<Command>> {
    let (commands, _rest) = decode_resp(input)?;
    if let DataType::Array(commands) = commands {
        let mut iter = commands.into_iter().peekable();
        let mut commands = vec![];
        while let Some(command) = iter.next() {
            match command {
                DataType::BulkString(ref s) | DataType::SimpleString(ref s) => {
                    // TODO: test this case insensitivity!
                    let command = match s.to_lowercase().as_str() {
                        "ping" => Command::Ping,
                        "del" => {
                            let mut keys: Vec<String> = vec![];
                            for key in iter.by_ref() {
                                match key {
                                    DataType::BulkString(key) | DataType::SimpleString(key) => {
                                        keys.push(key);
                                    }
                                    _ => {
                                        return Err(Error::other(format!(
                                            "invalid RESP `DEL` key {key:?}"
                                        )))
                                    }
                                }
                            }
                            Command::Del(keys)
                        }
                        "echo" => {
                            let echo_value = iter.next().expect("missing ECHO value");
                            match echo_value {
                                DataType::BulkString(echo_value)
                                | DataType::SimpleString(echo_value) => Command::Echo(echo_value),
                                _ => {
                                    return Err(Error::other(format!(
                                        "invalid RESP `ECHO` value {echo_value:?}"
                                    )))
                                }
                            }
                        }
                        "get" => {
                            let key = iter.next().expect("missing GET key");
                            let key = match key {
                                DataType::BulkString(key) | DataType::SimpleString(key) => key,
                                _ => {
                                    return Err(Error::other(format!(
                                        "invalid RESP `GET` key {key:?}"
                                    )))
                                }
                            };
                            Command::Get(key)
                        }
                        "info" => {
                            let mut sections = vec![];
                            for section in &mut iter {
                                match section {
                                    DataType::BulkString(section)
                                    | DataType::SimpleString(section) => {
                                        sections.push(section);
                                    }
                                    _ => {
                                        return Err(Error::other(format!(
                                            "invalid RESP `INFO` section {section:?}"
                                        )))
                                    }
                                }
                            }
                            Command::Info(sections)
                        }
                        "psync" => {
                            if let DataType::BulkString(replication_id) =
                                iter.next().expect("missing PSYNC replication_id")
                            {
                                if let DataType::BulkString(offset) =
                                    iter.next().expect("missing PSYNC offset")
                                {
                                    let offset =
                                        offset.parse::<i64>().expect("invalid PSYNC offset");
                                    Command::Psync(replication_id, offset)
                                } else {
                                    return Err(Error::other(
                                        "invalid RESP `PSYNC` offset".to_string(),
                                    ));
                                }
                            } else {
                                return Err(Error::other(
                                    "invalid RESP `PSYNC` replication_id".to_string(),
                                ));
                            }
                        }
                        "replconf" => {
                            let arg = iter.next().expect("missing REPLCONF argument");
                            let arg = match arg {
                                DataType::BulkString(arg) | DataType::SimpleString(arg) => arg,
                                _ => {
                                    return Err(Error::other(format!(
                                        "invalid RESP `REPLCONF` argument {arg:?}"
                                    )))
                                }
                            };
                            let val = iter.next().expect("missing REPLCONF argument value");
                            let val = match val {
                                DataType::BulkString(val) | DataType::SimpleString(val) => val,
                                _ => {
                                    return Err(Error::other(format!(
                                        "invalid RESP `REPLCONF` argument value {val:?}"
                                    )))
                                }
                            };
                            Command::ReplConf(arg, val)
                        }
                        "set" => {
                            let key = iter.next().expect("missing SET key");
                            let key = match key {
                                DataType::BulkString(key) | DataType::SimpleString(key) => key,
                                _ => {
                                    return Err(Error::other(format!(
                                        "invalid RESP `GET` key {key:?}"
                                    )))
                                }
                            };
                            let val = iter.next().expect("missing SET value");
                            let val = match val {
                                DataType::BulkString(val) | DataType::SimpleString(val) => val,
                                _ => {
                                    return Err(Error::other(format!(
                                        "invalid RESP `SET` val {val:?}"
                                    )))
                                }
                            };
                            if let Some(data) = iter.peek() {
                                match data {
                                    DataType::BulkString(ref s) | DataType::SimpleString(ref s) => {
                                        if s.to_lowercase().as_str() == "px" {
                                            iter.next();
                                            let millis = iter.next().expect("missing PX value");
                                            match millis {
                                                DataType::BulkString(millis)
                                                | DataType::SimpleString(millis) => {
                                                    let millis = millis
                                                        .parse::<u64>()
                                                        .expect("invalid PX value");
                                                    Command::Set(key, val, Some(millis))
                                                }
                                                _ => {
                                                    return Err(Error::other(format!(
                                                        "invalid RESP `SET` PX value {millis:?}"
                                                    )))
                                                }
                                            }
                                        } else {
                                            return Err(Error::other(format!(
                                                "invalid RESP `SET` option {s:?}"
                                            )));
                                        }
                                    }
                                    unknown => {
                                        return Err(Error::other(format!(
                                            "invalid RESP `SET` option {unknown:?}"
                                        )))
                                    }
                                }
                            } else {
                                Command::Set(key, val, None)
                            }
                        }
                        unknown => {
                            return Err(Error::other(format!(
                                "unknown command `{}`",
                                unknown.to_uppercase()
                            )))
                        }
                    };
                    commands.push(command);
                }
                _ => return Err(Error::other(format!("invalid RESP command {command:?}"))),
            };
        }
        Ok(commands)
    } else {
        eprintln!("invalid commands, expected RESP array, got {commands:?}");
        Err(Error::other(format!("invalid RESP commands: {commands:?}")))
    }
}

pub(crate) fn decode_resp(input: &str) -> Result<(DataType, &str)> {
    // check first byte. we'll handle +, $, * for now.
    let (first_byte, rest) = input.split_at(1);
    let first_char = first_byte.chars().next().expect("unexpected empty string");
    println!("first char: {first_char}, rest: {rest}");
    let data = match first_char {
        '+' => {
            // parse simple string
            let (simple_string, rest) = rest
                .split_once(CRLF)
                .expect("missing \\r\\n terminator for simple string");
            (DataType::SimpleString(simple_string.to_string()), rest)
        }
        '$' => {
            let (str_len, rest) = rest
                .split_once(CRLF)
                .expect("missing RESP terminator (`\r\n`) for bulk string");
            let len: usize = str_len.parse().expect("invalid bulk string length");
            println!("bulk string detected. len: {len}");
            let bulk_string = &rest[..len];
            println!("bulk string: {:?}", bulk_string);
            // skip \r\n & panic if not found
            let (crlf, rest) = rest.split_at(len + 2);
            assert_eq!(crlf, format!("{bulk_string}{CRLF}"));
            (DataType::BulkString(bulk_string.to_string()), rest)
        }
        '*' => {
            let (str_len, mut rest) = rest
                .split_once(CRLF)
                .expect("missing RESP terminator (`\r\n`) terminator for array");
            let len: usize = str_len.parse().expect("invalid array length");
            println!("array detected. len: {len}");
            // recursively parse array elements by \r\n
            let mut arr = Vec::with_capacity(len);
            for _ in 0..len {
                let (element, new_rest) = decode_resp(rest)?;
                println!("element: {element:?}     rest: {new_rest}");
                rest = new_rest;
                arr.push(element);
            }
            println!("array: {arr:?}   rest: {rest}");

            (DataType::Array(arr), rest)
        }
        _ => {
            eprintln!("unimplemented type: {:?}", first_char);
            unimplemented!()
        }
    };

    Ok(data)
}

#[derive(Clone)]
struct ReplicationClient {
    listening_port: u16,
    listening_host: IpAddr,
    state: ReplicationState,
    stream: Option<Arc<Mutex<TcpStream>>>,
}

#[derive(Copy, Clone)]
enum ReplicationState {
    Handshake,
    Sync,
    Connected,
}

/// Server replication modes
#[derive(Copy, Clone)]
enum Mode {
    Master,
    Replica(IpAddr, u16),
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Mode::Master => write!(f, "master"),
            Mode::Replica(_ip, _port) => write!(f, "slave"),
        }
    }
}

/// Redis commands
#[derive(Debug, Clone)]
pub(crate) enum Command {
    /// PING
    Ping,
    /// DEL key
    Del(Vec<String>),
    /// ECHO message
    Echo(String),
    /// GET key
    Get(String),
    /// INFO
    Info(Vec<String>),
    /// PSYNC
    Psync(String, i64),
    /// REPLCONF [arg val] e.g. listening-port 6380
    ReplConf(String, String),
    /// SET key value [PX milliseconds]
    Set(String, String, Option<u64>),
}

/// RESP (Redis serialization protocol) types - only supporting RESP2 for now.
#[derive(Debug, PartialEq)]
pub(crate) enum DataType {
    SimpleString(String),
    BulkString(String),
    Array(Vec<DataType>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    #[test]
    fn test_ping() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream.write_all(b"*1\r\n$4\r\nping\r\n").unwrap();
        let mut buf = [0; 512];
        let len = stream.read(&mut buf).unwrap();
        assert_eq!(len, 7);
        assert_eq!(String::from_utf8_lossy(&buf[..len]), "+PONG\r\n");
    }

    #[test]
    fn test_double_ping() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream.write_all(b"*1\r\n$4\r\nping\r\n").unwrap();
        stream.flush().unwrap();

        let mut buf = [0; 512];

        let len = stream.read(&mut buf).unwrap();
        assert_eq!(len, 7);
        assert_eq!(String::from_utf8_lossy(&buf[..len]), "+PONG\r\n");

        stream.write_all(b"*1\r\n$4\r\nping\r\n").unwrap();
        stream.flush().unwrap();
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!(len, 7);
        assert_eq!(String::from_utf8_lossy(&buf[..len]), "+PONG\r\n");
    }

    #[test]
    fn test_echo() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream
            .write_all(b"*2\r\n$4\r\necho\r\n$3\r\nhey\r\n")
            .unwrap();
        stream.flush().unwrap();
        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("$3\r\nhey\r\n", String::from_utf8_lossy(&buf[..len]));
    }

    #[test]
    fn test_set_and_get() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream
            .write_all(b"*3\r\n$3\r\nset\r\n$3\r\nmsg\r\n$3\r\nhey\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("+OK\r\n", String::from_utf8_lossy(&buf[..len]));

        stream
            .write_all(b"*2\r\n$3\r\nget\r\n$3\r\nmsg\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("$3\r\nhey\r\n", String::from_utf8_lossy(&buf[..len]));
    }

    #[test]
    fn test_del() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream
            .write_all(b"*3\r\n$3\r\nset\r\n$5\r\nshort\r\n$3\r\nsup\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("+OK\r\n", String::from_utf8_lossy(&buf[..len]));

        stream
            .write_all(b"*2\r\n$3\r\nget\r\n$5\r\nshort\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("$3\r\nsup\r\n", String::from_utf8_lossy(&buf[..len]));

        stream
            .write_all(b"*2\r\n$3\r\ndel\r\n$5\r\nshort\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!(":1\r\n", String::from_utf8_lossy(&buf[..len]));

        stream
            .write_all(b"*2\r\n$3\r\nget\r\n$5\r\nshort\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("$-1\r\n", String::from_utf8_lossy(&buf[..len]));
    }

    #[test]
    fn test_set_with_px() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream
            .write_all(b"*5\r\n$3\r\nset\r\n$3\r\ntmp\r\n$3\r\nhey\r\n$2\r\npx\r\n$2\r\n50\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("+OK\r\n", String::from_utf8_lossy(&buf[..len]));

        stream
            .write_all(b"*2\r\n$3\r\nget\r\n$3\r\ntmp\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("$3\r\nhey\r\n", String::from_utf8_lossy(&buf[..len]));

        std::thread::sleep(Duration::from_millis(100));

        stream
            .write_all(b"*2\r\n$3\r\nget\r\n$3\r\ntmp\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("$-1\r\n", String::from_utf8_lossy(&buf[..len]));
    }

    #[test]
    fn test_get_nonexistent_key_returns_resp_null() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream
            .write_all(b"*2\r\n$3\r\nget\r\n$11\r\ndoesntexist\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("$-1\r\n", String::from_utf8_lossy(&buf[..len]));
    }

    #[test]
    fn test_info_replication() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream
            .write_all(b"*2\r\n$4\r\ninfo\r\n$11\r\nreplication\r\n")
            .unwrap();
        stream.flush().unwrap();
        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        let str = std::str::from_utf8(&buf[..len]).unwrap();
        let (parsed, _) = decode_resp(str).unwrap();
        if let DataType::BulkString(s) = parsed {
            assert!(s.contains("role:master"));
            assert!(s.contains("connected_slaves:1"));
            assert!(s.contains("master_replid:"));
            assert!(s.contains("master_repl_offset:0"));
        } else {
            panic!("Expected bulk string");
        }
    }

    #[test]
    fn test_replconf() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream
            .write_all(b"*3\r\n$8\r\nreplconf\r\n$14\r\nlistening-port\r\n$4\r\n1234\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        stream.flush().unwrap();
        assert_eq!("+OK\r\n", String::from_utf8_lossy(&buf[..len]));
    }

    #[test]
    fn test_psync() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream
            .write_all(b"*3\r\n$5\r\npsync\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            .unwrap();
        stream.flush().unwrap();

        let mut buf = vec![0; 512];
        let len = stream.read(&mut buf).unwrap();
        let str = std::str::from_utf8(&buf[..len]).unwrap();
        let (parsed, _) = decode_resp(str).unwrap();
        if let DataType::SimpleString(s) = parsed {
            assert!(s.starts_with("FULLRESYNC"));
            assert!(s.contains(" 0"));
        } else {
            panic!("Expected simple string, got {parsed:?}");
        }
    }

    #[test]
    fn test_decode_resp_simple_string() {
        let data = "+OK\r\n";
        let (parsed, _rest) = decode_resp(data).unwrap();
        assert_eq!(parsed, DataType::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_decode_resp_bulk_string() {
        let data = "$5\r\nhello\r\n";
        let (parsed, _rest) = decode_resp(data).unwrap();
        assert_eq!(parsed, DataType::BulkString("hello".to_string()));
    }

    #[test]
    fn test_decode_resp_array() {
        let data = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (parsed, _rest) = decode_resp(data).unwrap();
        assert_eq!(
            parsed,
            DataType::Array(vec![
                DataType::BulkString("foo".to_string()),
                DataType::BulkString("bar".to_string())
            ])
        );
    }
}
