use binance_connector;
use binance_connector::config::Config;
use binance_connector::connector::{print_message, process_message, subscribe, Message, SubscriptionType};
use futures_util::future::join_all;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;


// Here we have single order book which is build from updates
// from many connections taking the latest (best) updates and ignoring others
#[allow(dead_code)]
async fn one_order_book_with_many_connections(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(Config::build(&args)?);

    let (u_tx, u_rx) = mpsc::channel::<Message>(1024);
    let (ob_tx, ob_rx) = mpsc::channel::<Message>(1024);

    let token = CancellationToken::new();

    let futures: Vec<Pin<Box<dyn Future<Output=Result<(), Box<dyn std::error::Error>>>>>> = vec![
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx.clone(), token.clone())),
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx.clone(), token.clone())),
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx.clone(), token.clone())),
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx, token.clone())),
        Box::pin(process_message(config.clone(), u_rx, ob_tx)),
        Box::pin(print_message(ob_rx)),
    ];

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        token.cancel();
    });

    join_all(futures).await;

    Ok(())
}

// Here we have several pairs (subscriber, processor) and they are keeping their own order book.
// Printer prints an order book with the best id and skipping others
#[allow(dead_code)]
async fn many_order_books_with_many_connections(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(Config::build(&args)?);

    let (u_tx1, u_rx1) = mpsc::channel::<Message>(1024);
    let (u_tx2, u_rx2) = mpsc::channel::<Message>(1024);
    let (u_tx3, u_rx3) = mpsc::channel::<Message>(1024);
    let (u_tx4, u_rx4) = mpsc::channel::<Message>(1024);
    let (ob_tx, ob_rx) = mpsc::channel::<Message>(1024);

    let token = CancellationToken::new();

    let futures: Vec<Pin<Box<dyn Future<Output=Result<(), Box<dyn std::error::Error>>>>>> = vec![
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx1, token.clone())),
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx2, token.clone())),
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx3, token.clone())),
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx4, token.clone())),
        Box::pin(process_message(config.clone(), u_rx1, ob_tx.clone())),
        Box::pin(process_message(config.clone(), u_rx2, ob_tx.clone())),
        Box::pin(process_message(config.clone(), u_rx3, ob_tx.clone())),
        Box::pin(process_message(config, u_rx4, ob_tx)),
        Box::pin(print_message(ob_rx))
    ];

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        token.cancel();
    });

    join_all(futures).await;

    Ok(())
}

#[allow(dead_code)]
async fn order_book_and_trades(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(Config::build(&args)?);

    let (u_tx, u_rx) = mpsc::channel::<Message>(1024);
    let (ob_tx, ob_rx) = mpsc::channel::<Message>(1024);

    let token = CancellationToken::new();

    let futures: Vec<Pin<Box<dyn Future<Output=Result<(), Box<dyn std::error::Error>>>>>> = vec![
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx.clone(), token.clone())),
        Box::pin(subscribe(config.clone(), SubscriptionType::OrderBook, u_tx.clone(), token.clone())),
        Box::pin(subscribe(config.clone(), SubscriptionType::Trades, u_tx, token.clone())),
        Box::pin(process_message(config.clone(), u_rx, ob_tx)),
        Box::pin(print_message(ob_rx)),
    ];

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        token.cancel();
    });

    join_all(futures).await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    // 3 examples, use one !
    one_order_book_with_many_connections(args).await?;
    //many_order_books_with_many_connections(args).await?;
    //order_book_and_trades(args).await?;

    Ok(())
}