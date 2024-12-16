use crate::config::Config;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::json;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

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
                       subscription: SubscriptionType,
                       tx: Sender<Message>) -> Result<(), Box<dyn std::error::Error>> {
    let ws = connect(&config.update_url).await?;
    let (mut writer, reader) = ws.split();

    let msg = gen_subscribe_msg(&config.symbol, &subscription);
    writer.send(msg.into()).await?; // TODO

    reader.for_each(move |message| {
        let tx = tx.clone();
        let subscription = subscription.clone();
        async move {
            let data = message.unwrap().into_text().unwrap(); // TODO
            match subscription {
                SubscriptionType::Trades => {
                    if let Ok(trade) = serde_json::from_str::<Trade>(&data) {
                        let _ = tx.send(Message::TradeUpdate(trade)).await;
                    } else {
                        //
                    }
                }
                SubscriptionType::OrderBook => {
                    if let Ok(update) = serde_json::from_str::<OBUpdate>(&data) {
                        let _ = tx.send(Message::OrderBookUpdate(update)).await;
                    } else {
                        //
                    }
                }
            }
        }
    }).await;

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
                            let snapshot = get_order_book_snapshot(config.clone()).await?;
                            ob.process_snapshot(snapshot)?;
                        } else {
                            tx.send(Message::OrderBook(ob.clone())).await?;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error processing update: {:?}", e);
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

fn gen_subscribe_msg(symbol: &str, subscription: &SubscriptionType) -> String {
    let msg = match subscription {
        SubscriptionType::Trades => {
            json!({
            "method": "SUBSCRIBE",
            "params": [
                format!("{}@aggTrade", symbol)
            ],
            "id": 1
        })
        }
        SubscriptionType::OrderBook => {
            json!({
            "method": "SUBSCRIBE",
            "params": [
                format!("{}@depth@100ms", symbol)
            ],
            "id": 1
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

