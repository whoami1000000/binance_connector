use crate::config::Config;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::json;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type OB = crate::order_book::OrderBook<Decimal, Decimal>;
type OBUpdate = crate::order_book::Update<Decimal, Decimal>;
type OBSnapshot = crate::order_book::Snapshot<Decimal, Decimal>;
type Trade = crate::trade::Trade<Decimal, Decimal>;

#[derive(Clone)]
pub enum SubscriptionType {
    Trades,
    OrderBook,
}

pub enum Message {
    OrderBookUpdate(OBUpdate),
    OrderBook(OB),
    TradeUpdate(Trade),
}

pub async fn subscribe(config: Arc<Config>,
                       subscription_type: SubscriptionType,
                       tx: Sender<Message>,
                       token: CancellationToken) -> Result<(), Box<dyn std::error::Error>> {
    'outer: while !token.is_cancelled() {
        println!("connecting to {}", config.update_url);
        let connection = connect(&config.update_url).await;
        match connection {
            Ok(connection) => {
                let (mut writer, mut reader) = connection.split();

                let subscription = create_subscription(&config.symbol, &subscription_type);
                if let Err(e) = writer.send(subscription.into()).await {
                    eprintln!("error sending subscription request: {:?}", e);
                    continue; // reconnect
                }

                while let Some(Ok(msg)) = reader.next().await {
                    if token.is_cancelled() {
                        break 'outer;
                    }

                    match msg {
                        tokio_tungstenite::tungstenite::Message::Text(data) => {
                            match subscription_type {
                                SubscriptionType::Trades => {
                                    if let Ok(trade) = serde_json::from_str::<Trade>(&data) {
                                        tx.send(Message::TradeUpdate(trade)).await?
                                    } else {
                                        eprintln!("error deserializing trades: {:?}", data);
                                    }
                                }
                                SubscriptionType::OrderBook => {
                                    if let Ok(update) = serde_json::from_str::<OBUpdate>(&data) {
                                        tx.send(Message::OrderBookUpdate(update)).await?
                                    } else {
                                        eprintln!("error deserializing order book: {:?}", data);
                                    }
                                }
                            }
                        }
                        tokio_tungstenite::tungstenite::Message::Ping(data) => {
                            println!("Ping: {:?}", data);
                            // we should send the same content back by binance requirements
                            writer.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await?;
                        }
                        tokio_tungstenite::tungstenite::Message::Close(_) => {
                            println!("close");
                            break; // reconnect
                        }
                        _ => continue,
                    }
                }
            }
            Err(e) => {
                eprintln!("couldn't connect to websocket due to {:?}", e);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue; // try again
            }
        }
    }

    println!("cancelled subscription");
    Ok(())
}
pub async fn process_message(config: Arc<Config>, mut rx: Receiver<Message>, tx: Sender<Message>) -> Result<(), Box<dyn std::error::Error>> {
    let mut ob = OB::build(&config.symbol, config.depth)?;

    while let Some(msg) = rx.recv().await {
        match msg {
            Message::OrderBookUpdate(update) => {
                match ob.process_update(update) {
                    Ok(_) => {
                        if !ob.has_snapshot() {
                            if let Ok(snapshot) = get_order_book_snapshot(config.clone()).await {
                                ob.process_snapshot(snapshot)?; // TODO
                            }
                        } else {
                            tx.send(Message::OrderBook(ob.clone())).await?;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error processing message: {:?}", e);
                        ob.reset();
                    }
                }
            }
            Message::TradeUpdate(trade) => {
                tx.send(Message::TradeUpdate(trade)).await?;
            }
            _ => {
                unreachable!();
            }
        }
    }

    Ok(())
}

pub async fn print_message(mut rx: Receiver<Message>) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_id: u64 = 0;
    while let Some(msg) = rx.recv().await {
        match &msg {
            Message::OrderBook(ob) => {
                if ob.last_update_id() > last_id { // print the best order book !
                    last_id = ob.last_update_id();
                    tokio::io::stdout().write(ob.to_string().as_bytes()).await?;
                }
            }
            Message::TradeUpdate(trade) => {
                tokio::io::stdout().write(trade.to_string().as_bytes()).await?;
            }
            _ => {
                unreachable!();
            }
        }
    }

    Ok(())
}

fn create_subscription(symbol: &str, subscription: &SubscriptionType) -> String {
    let id = Uuid::new_v4();
    let msg = match subscription {
        SubscriptionType::Trades => {
            json!({
                "method": "SUBSCRIBE",
                "params": [
                    format!("{}@aggTrade", symbol)
                ],
                "id": id.to_string()
            })
        }
        SubscriptionType::OrderBook => {
            json!({
                "method": "SUBSCRIBE",
                "params": [
                    format!("{}@depth@100ms", symbol)
                ],
                "id": id.to_string()
            })
        }
    };

    msg.to_string()
}

async fn connect(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, tokio_tungstenite::tungstenite::Error> {
    match connect_async(url).await {
        Ok((stream, _)) => Ok(stream),
        Err(e) => Err(e)
    }
}

async fn get_order_book_snapshot(config: Arc<Config>) -> Result<OBSnapshot, Box<dyn std::error::Error>> {
    let rsp = reqwest::get(&config.snapshot_url).await?;
    let body = rsp.text().await?;
    let snapshot = serde_json::from_str::<OBSnapshot>(&body)?;
    Ok(snapshot)
}
