use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        println!("accepted new connection");
        let mut buf = [0; 512];
        loop {
            if let Ok(len) = stream.read(&mut buf) {
                if len == 0 {
                    break;
                }
                println!("received: {:?}", String::from_utf8_lossy(&buf[..len]));
                let response = match &buf[..len] {
                    b"*1\r\n$4\r\nping\r\n" => "+PONG\r\n",
                    _ => "-ERR unknown command\r\n",
                };
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
