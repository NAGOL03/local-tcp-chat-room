use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, _) = broadcast::channel(16);
    let listener = TcpListener::bind("192.168.1.90:7070").await?;

    loop {
        // Clone sender to send to channel and also subscribe
        // to listen to all senders in the channel.
        let (mut socket, _) = listener.accept().await?;
        let sender = sender.clone();
        let mut receiver = sender.subscribe();
        tokio::spawn(async move {
            loop {
                let mut buf = vec![0u8; 2048];
                tokio::select! {
                    read_bytes = socket.read(&mut buf) => {
                        match read_bytes {
                            Ok(bytes) => {
                                buf.truncate(bytes);
                                sender.send(buf).unwrap();
                            }
                            Err(e) => {
                                eprintln!("Error reading:{:?}", e);
                                ()
                            }
                        }
                    }
                    write_bytes = receiver.recv() => {
                        match write_bytes {
                            Ok(buf) => {
                                socket.write_all(&buf).await.unwrap();
                            }
                            Err(e) => {
                                eprintln!("Error writing:{:?}", e);
                                ()
                            }
                        }
                    }
                }
            }
        });
    }
}
