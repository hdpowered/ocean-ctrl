use std::sync::OnceLock;

use anyhow::{Context, Result};
use tap::Pipe;

static ACCESS_PASSWORD: OnceLock<String> = OnceLock::new();

pub fn access_password() -> &'static str {
    ACCESS_PASSWORD
        .get()
        .expect("ACCESS_PASSWORD has to be initialized")
}

pub fn init() -> Result<()> {
    std::env::var("ACCESS_PASSWORD")
        .context("ACCESS_PASSWORD environment variable is not set")?
        .pipe(|v| ACCESS_PASSWORD.set(v))
        .expect("Failed to set ACCESS_PASSWORD");

    Ok(())
}
