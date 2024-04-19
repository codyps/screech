use eyre::Context;
use std::{collections::HashMap, net::SocketAddr};
use tokio_util::codec::Decoder;
use tracing::warn;

use tokio::net::ToSocketAddrs;

mod msg;

struct Client {
    // TODO: allow for unix sockets
    // TODO: allow re-resolving addresses on reconnects.
    // TODO: servers can provide us with a list of addresses, which we _probably_ should then use.
    // Consider if they should entirely replace the list we were using previously or if we should
    // keep the launch list.
    // TODO: consider allowing other sources of dynamic modification of the address list.
    addrs: Vec<SocketAddr>,

    subs: HashMap<String, Vec<SocketAddr>>,
}

impl Client {
    async fn connect<A, T>(addrs: A) -> Result<Self, Error>
    where
        A: IntoIterator<Item = T> + 'static + Send + Sync,
        T: ToSocketAddrs + 'static + Send + Sync,
    {
        // TODO: all the error propagation here needs more detail

        let mut resolvers = tokio::task::JoinSet::new();
        for addr in addrs {
            resolvers.spawn(async move { tokio::net::lookup_host(addr).await });
        }

        let mut connectors = tokio::task::JoinSet::new();

        while let Some(res) = resolvers.join_next().await {
            match res {
                Ok(Ok(addrs)) => {
                    for addr in addrs {
                        connectors.spawn(async move {
                            let con = tokio::net::TcpStream::connect(&addr)
                                .await
                                .with_context(|| format!("failed to connect to {:?}", addr))?;

                            // TODO: wrap connection in encode/decode layer with cr/lf seperated lines

                            Ok::<(), eyre::Report>(())
                        });
                    }
                }
                Ok(Err(e)) => {
                    warn!("Failed to resolve address: {:?}", e);
                }
                Err(e) => {
                    warn!("Spawn failure while resolving addr: {:?}", e);
                }
            }
        }

        Ok(Self {
            addrs: Vec::new(),
            subs: HashMap::new(),
        })
    }
}

struct Server {}

impl Server {
    fn new() -> Self {
        Self {}
    }

    async fn run(self) {
        println!("Server is running");
    }
}

struct NatsDecoder {
    /// Last index we read to check for a `cr/lf`. Never advanced into a `cr` so we can
    /// always scan for the `cr/lf` pair.
    last_read_pos: usize,
    // TODO: consider max payload size
}

impl Decoder for NatsDecoder {
    // TODO: parse out a message from the bytes
    type Item = &[u8];
    type Error = Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let pos = memchr::memmem::find(src[self.last_read_pos..], b"\r\n");

        if let Some(pos) = pos {
            let line = src.split_to(pos + 2);
            self.last_read_pos = 0;

            Ok(Some(&line))
        } else {
            if src[self.src.len() - 1] == b'\r' {
                self.last_read_pos = src.len() - 1;
            } else {
                self.last_read_pos = src.len();
            }

            Ok(None)
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    // FIXME: eliminate these
    Eyre(#[from] eyre::Report),
}
