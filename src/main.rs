mod models;

use std::fs;

use anyhow::{Context, Result};
use axum::{Router, routing::get};
use const_format::formatcp;
use tap::Pipe;

use crate::models::AppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let config = {
        const CONFIG_FILEPATH: &str = "config.toml";
        fs::read_to_string("config.toml")
            .context(formatcp!("Failed to read {CONFIG_FILEPATH}"))?
            .pipe_borrow(toml::from_str::<AppConfig>)
            .context(formatcp!("Failed to parse {CONFIG_FILEPATH}"))?
    };

    let app = Router::new().route("/", get(root));

    let listener = {
        let addr = format!("{}:{}", config.host, config.port);
        tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("Failed to bind address {addr}"))?
    };

    axum::serve(listener, app)
        .await
        .context("Failed to start server")?;

    Ok(())
}

async fn root() -> String {
    "Hello".to_owned()
}
