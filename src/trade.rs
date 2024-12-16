use chrono::{MappedLocalTime, TimeZone, Utc};
use serde::Deserialize;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Deserialize)]
pub struct Trade<P, Q> {
    #[serde(alias = "E")]
    event_time: i64,

    #[serde(alias = "s")]
    symbol: String,

    #[serde(alias = "a")]
    trade_id: u64,

    #[serde(alias = "p")]
    price: P,

    #[serde(alias = "q")]
    quantity: Q,

    #[serde(alias = "f")]
    first_trade_id: u64,

    #[serde(alias = "l")]
    last_trade_id: u64,

    #[serde(alias = "T")]
    trade_time: i64,

    #[serde(alias = "m")]
    is_market_maker: bool,
}
impl<P: Display, Q: Display> Display for Trade<P, Q> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Trade\n")?;
        write!(f, "Symbol: {}\n", self.symbol)?;
        write!(f, "Id: {}\n", self.trade_id)?;
        if let MappedLocalTime::Single(dt) = Utc.timestamp_millis_opt(self.event_time) {
            write!(f, "Event Time: {}\n", dt.to_rfc3339())?;
        }
        if let MappedLocalTime::Single(dt) = Utc.timestamp_millis_opt(self.trade_time) {
            write!(f, "Trade Time: {}\n", dt.to_rfc3339())?;
        }
        write!(f, "Price: {}\n", self.price)?;
        write!(f, "Quantity: {}\n", self.quantity)?;
        write!(f, "First Id: {}\n", self.first_trade_id)?;
        write!(f, "Last Id: {}\n", self.last_trade_id)?;
        write!(f, "Market Maker: {}\n", self.is_market_maker)?;
        write!(f, "\n")
    }
}
