const UPDATE_URL: &str = "wss://stream.binance.com:443/ws";
const BINANCE_REST_API_URL: &str = "https://api.binance.com/api/v3";

#[derive(Clone)]
pub struct Config {
    pub update_url: String,
    pub snapshot_url: String,
    pub symbol: String,
    pub depth: usize,
}

impl Config {
    pub fn build(args: &[String]) -> Result<Config, Box<dyn std::error::Error>> {
        if args.len() < 2 {
            return Err(Box::from("not enough arguments"));
        }

        let symbol = args[1].clone();

        let mut depth = 10;
        if args.len() >= 3 {
            depth = args[2].parse::<usize>()?;
        }

        let update_url = UPDATE_URL.to_string();
        let snapshot_url = format!("{}/depth?symbol={}&limit={}", BINANCE_REST_API_URL, symbol.to_uppercase(), depth);

        Ok(Config {
            update_url,
            snapshot_url,
            symbol,
            depth,
        })
    }
}