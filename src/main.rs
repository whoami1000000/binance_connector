use futures_util::{SinkExt, StreamExt};
use order_book::config::Config;
use order_book::order_book::{OrderBook, Snapshot, Update};
use rust_decimal::Decimal;
use serde_json::json;
use std::process::exit;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type OB = OrderBook<Decimal, Decimal>;


async fn connect(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, tokio_tungstenite::tungstenite::Error> {
    match connect_async(url).await {
        Ok((stream, _)) => Ok(stream),
        Err(e) => Err(e)
    }
}

async fn handle_update(message: tungstenite::Message,
                       ob: Arc<Mutex<OB>>,
                       tx: Sender<OB>,
                       notificator: Arc<Notify>) -> Result<(), Box<dyn std::error::Error>> {
    let data = message.into_text()?;

    let update = serde_json::from_str::<Update<Decimal, Decimal>>(&data)?;

    let mut msg: Option<OB> = None;
    {
        let mut ob = ob.lock().unwrap();
        ob.process_update(update).unwrap();
        if ob.has_snapshot() {
            msg = Some(ob.clone());
        } else {
            notificator.notify_one();
        }
    }

    if let Some(msg) = msg {
        tx.send(msg).await?;
    }

    Ok(())
}

async fn subscribe(ob: Arc<Mutex<OB>>,
                   config: Arc<Config>,
                   tx: Sender<OB>,
                   notificator: Arc<Notify>) -> Result<(), Box<dyn std::error::Error>> {
    let ws = connect(&config.update_url).await?;
    let (mut writer, reader) = ws.split();

    let msg = gen_subscribe_msg(&config.symbol);
    writer.send(msg.into()).await?;

    let cloned_ob = Arc::clone(&ob);
    reader.for_each(move |message| {
        let ob = cloned_ob.clone();
        let tx = tx.clone();
        let notificator = notificator.clone();
        async {
            handle_update(message.unwrap(), ob, tx, notificator).await.unwrap_or_else(|_e| {
                // TODO:
            });
        }
    }).await;

    Ok(())
}

async fn get_snapshot(config: Arc<Config>) -> Result<Snapshot<Decimal, Decimal>, Box<dyn std::error::Error>> {
    let rsp = reqwest::get(&config.snapshot_url).await?;
    let body = rsp.text().await?;
    let snapshot = serde_json::from_str::<Snapshot<Decimal, Decimal>>(&body)?;
    Ok(snapshot)
}

fn gen_subscribe_msg(symbol: &str) -> String {
    let msg = json!({
            "method": "SUBSCRIBE",
            "params": [
                format!("{}@depth@100ms", symbol)
            ],
            "id": 1
        });
    msg.to_string()
}

async fn run(config: Arc<Config>, tx: Sender<OB>) -> Result<(), Box<dyn std::error::Error>> {
    let notificator = Arc::new(Notify::new());

    let ob = Arc::new(Mutex::new(OB::build(config.depth)?));

    let updater = subscribe(
        Arc::clone(&ob),
        Arc::clone(&config),
        mpsc::Sender::clone(&tx),
        Arc::clone(&notificator));

    let snapshot = async {
        notificator.notified().await;
        let snap = get_snapshot(config).await.unwrap();
        let mut ob = ob.lock().unwrap();
        ob.process_snapshot(snap).unwrap();
    };

    let _ = tokio::join!(updater, snapshot);

    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let config = Arc::new(Config::build(&args)?);

    let (tx, mut rx) = mpsc::channel::<OB>(1024);

    let printer = async {
        let mut last_id: u64 = 0;
        while let Some(ob) = rx.recv().await {
            if ob.last_update_id() > last_id { // print the best order book !
                last_id = ob.last_update_id();
                tokio::io::stdout().write(ob.to_string().as_bytes()).await.unwrap();
            }
        }
    };

    let updater = run(config.clone(), tx.clone());

    tokio::spawn(async {
        tokio::signal::ctrl_c().await.unwrap();
        exit(0);
    });

    let _ = tokio::join!(printer, updater);

    Ok(())

    /*let mut ob = OrderBook::<i32, i32>::build(2).unwrap();

    let u1 = Update {
        first_update_id: 1,
        final_update_id: 3,
        previous_final_update_id: 0,
        bids: vec![(11, 1), (12, 1)],
        asks: vec![(21, 1), (22, 1)],
    };
    let u2 = Update {
        first_update_id: 4,
        final_update_id: 6,
        previous_final_update_id: 3,
        bids: vec![(10, 2), (11, 2)],
        asks: vec![(20, 2), (21, 2)],
    };

    ob.process_update(u1);
    ob.process_update(u2);

    let snapshot = Snapshot {
        last_update_id: 5,
        bids: vec![(9, 3), (10, 4)],
        asks: vec![(19, 4), (20, 3)],
    };
    ob.process_snapshot(snapshot);

    println!("{}", ob);*/
}