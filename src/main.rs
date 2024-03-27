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
            let mut buf = [0; 512];
            loop {
                if let Ok(len) = stream.read(&mut buf).await {
                    if len == 0 {
                        break;
                    }
                    println!("received: {:?}", String::from_utf8_lossy(&buf[..len]));
                    let response = match &buf[..len] {
                        b"*1\r\n$4\r\nping\r\n" => "+PONG\r\n",
                        _ => "-ERR unknown command\r\n",
                    };
                    stream.write_all(response.as_bytes()).await.unwrap();
                    stream.flush().await.unwrap();
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
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
}
