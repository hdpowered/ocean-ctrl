use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
}
