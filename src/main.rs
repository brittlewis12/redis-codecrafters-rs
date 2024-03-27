use std::{
    net::TcpListener,
    io::{Read, Write},
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buf = [0; 512];
                stream.read(&mut buf).unwrap();
                println!("received: {:?}", buf);
                let response = match &buf[..] {
                    b"*1\r\n$4\r\nping\r\n" => "+PONG\r\n",
                    _ => "-ERR unknown command\r\n",
                };
                stream.write(response.as_bytes()).unwrap();
                stream.flush().unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream;
    use std::io::Write;

    #[test]
    fn test_server() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream.write(b"GET / HTTP/1.1\r\n").unwrap();
    }

    #[test]
    fn test_server2() {
        let mut stream = TcpStream::connect("127.0.0.1:6379").unwrap();
        stream.write(b"*1\r\n$4\r\nping\r\n").unwrap();
    }
}
