use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Result},
    net::TcpListener,
};

const CRLF: &str = "\r\n";

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("listening on {}", listener.local_addr().unwrap());

    loop {
        let (mut stream, _addr) = listener.accept().await.unwrap();
        println!("accepted new connection");
        tokio::spawn(async move {
            loop {
                let mut buf = vec![0; 512];
                let len = stream.read(&mut buf).await.unwrap();
                if len == 0 {
                    break;
                }
                let input = std::str::from_utf8(&buf[..len]).expect("invalid utf8 string");
                println!("received: {:?}", input);
                let commands = parse(&input).unwrap_or_else(|e| {
                    eprintln!("error parsing input: {:?}", e);
                    vec![]
                });
                if commands.is_empty() {
                    stream
                        .write_all(format!("-ERR invalid request formatting{CRLF}").as_bytes())
                        .await
                        .unwrap();
                    stream.flush().await.unwrap();
                    break;
                }
                println!("commands: {:?}", commands);
                let mut responses = vec![];
                let mut commands = commands.iter();
                while let Some(command) = commands.next() {
                    println!("command: {:?}", command);
                    let response = match command {
                        Command::Ping => format!("+PONG{CRLF}"), // TODO: leverage serde, a la serde_bencode -- serde_resp?
                        Command::Echo(echo_value) => {
                            let len = echo_value.len();
                            format!("${len}{CRLF}{echo_value}{CRLF}")
                        }
                    };
                    responses.push(response);
                }
                for response in responses {
                    stream.write_all(response.as_bytes()).await.unwrap();
                    stream.flush().await.unwrap();
                }
            }
        });
    }
}

pub(crate) fn parse(input: &str) -> Result<Vec<Command>> {
    let (commands, _rest) = decode_resp(input)?;
    if let DataType::Array(commands) = commands {
        let mut iter = commands.into_iter();
        let mut commands = vec![];
        while let Some(command) = iter.next() {
            match command {
                DataType::BulkString(ref s) | DataType::SimpleString(ref s) => {
                    // TODO: test this case insensitivity!
                    let command = match s.to_lowercase().as_str() {
                        "ping" => Command::Ping,
                        "echo" => {
                            let echo_value = iter.next().expect("missing echo value");
                            match echo_value {
                                DataType::BulkString(echo_value)
                                | DataType::SimpleString(echo_value) => Command::Echo(echo_value),
                                _ => {
                                    return Err(std::io::Error::other(format!(
                                        "invalid RESP echo value {echo_value:?}"
                                    )))
                                }
                            }
                        }
                        _ => {
                            return Err(std::io::Error::other(format!(
                                "unknown command {command:?}"
                            )))
                        }
                    };
                    commands.push(command);
                }
                _ => {
                    return Err(std::io::Error::other(format!(
                        "invalid RESP command {command:?}"
                    )))
                }
            };
        }
        Ok(commands)
    } else {
        eprintln!("invalid commands, expected RESP array, got {commands:?}");
        Err(std::io::Error::other("invalid RESP commands"))
    }
}

pub(crate) fn decode_resp(input: &str) -> Result<(DataType, &str)> {
    // check first byte. we'll handle +, $, * for now.
    let (first_byte, rest) = input.split_at(1);
    let first_char = first_byte.chars().next().expect("unexpected empty string");
    println!("first char: {}", &first_char);
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
                let (element, new_rest) = decode_resp(&rest)?;
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

/// Redis commands
#[derive(Debug)]
pub(crate) enum Command {
    Ping,
    Echo(String),
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
        assert_eq!("$3\r\nhey\r\n", String::from_utf8_lossy(&mut buf[..len]));
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
