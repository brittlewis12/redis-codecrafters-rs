use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Result},
    net::TcpListener,
};

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
                let (commands, _rest) = parse(&input).unwrap();
                println!("commands: {:?}", commands);
                let mut responses = vec![];
                if let DataType::Array(commands) = commands {
                    let mut iter = commands.into_iter();
                    while let Some(command) = iter.next() {
                        println!("command: {:?}", command);
                        let response = match command {
                            DataType::BulkString(s) | DataType::SimpleString(s) => {
                                // TODO: test this case insensitivity!
                                match s.to_lowercase().as_str() {
                                    "ping" => "+PONG\r\n".into(), // TODO: leverage serde, a la serde_bencode -- serde_resp?
                                    "echo" => {
                                        if let DataType::BulkString(echo_value) =
                                            iter.next().expect("missing echo value")
                                        {
                                            format!("+{echo_value}\r\n")
                                        } else {
                                            // FIXME: better simple error
                                            "-ERR missing echo value\r\n".into()
                                        }
                                    }
                                    _ => "-ERR unknown command\r\n".into(),
                                }
                            }
                            _ => "-ERR unknown command\r\n".into(),
                        };
                        responses.push(response);
                    }
                    for response in responses {
                        stream.write_all(response.as_bytes()).await.unwrap();
                        stream.flush().await.unwrap();
                    }
                } else {
                    eprintln!("invalid command {commands:?}");
                    stream
                        .write_all("-ERR invalid request formatting\r\n".as_bytes())
                        .await
                        .unwrap();
                    stream.flush().await.unwrap();
                }
            }
        });
    }
}

pub(crate) fn parse(input: &str) -> Result<(DataType, &str)> {
    // check first byte. we'll handle +, $, * for now.
    let (first_byte, rest) = input.split_at(1);
    let first_char = first_byte.chars().next().expect("unexpected empty string");
    println!("first char: {}", &first_char);
    let data = match first_char {
        '+' => {
            // parse simple string
            let (simple_string, rest) = rest
                .split_once("\r\n")
                .expect("missing \\r\\n terminator for simple string");
            (DataType::SimpleString(simple_string.to_string()), rest)
        }
        '$' => {
            let (str_len, rest) = rest
                .split_once("\r\n")
                .expect("missing \\r\\n terminator for bulk string");
            let len: usize = str_len.parse().expect("invalid bulk string length");
            println!("bulk string detected. len: {len}");
            let bulk_string = &rest[..len];
            println!("bulk string: {:?}", bulk_string);
            // skip \r\n & panic if not found
            let (crlf, rest) = rest.split_at(len + 2);
            assert_eq!(crlf, format!("{bulk_string}\r\n"));
            (DataType::BulkString(bulk_string.to_string()), rest)
        }
        '*' => {
            let (str_len, mut rest) = rest
                .split_once("\r\n")
                .expect("missing \\r\\n terminator for array");
            let len: usize = str_len.parse().expect("invalid array length");
            println!("array detected. len: {len}");
            // recursively parse array elements by \r\n
            let mut arr = Vec::with_capacity(len);
            for _ in 0..len {
                let (element, new_rest) = parse(&rest)?;
                println!("element: {element:?}     rest: {rest}");
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
        assert_eq!("+hey\r\n", String::from_utf8_lossy(&mut buf[..len]));
    }

    #[test]
    fn test_parse_simple_string() {
        let data = "+OK\r\n";
        let (parsed, _rest) = parse(data).unwrap();
        assert_eq!(parsed, DataType::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_parse_bulk_string() {
        let data = "$5\r\nhello\r\n";
        let (parsed, _rest) = parse(data).unwrap();
        assert_eq!(parsed, DataType::BulkString("hello".to_string()));
    }

    #[test]
    fn test_parse_array() {
        let data = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (parsed, _rest) = parse(data).unwrap();
        assert_eq!(
            parsed,
            DataType::Array(vec![
                DataType::BulkString("foo".to_string()),
                DataType::BulkString("bar".to_string())
            ])
        );
    }
}
