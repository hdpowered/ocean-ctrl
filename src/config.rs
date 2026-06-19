use std::sync::OnceLock;

use anyhow::{Context, Result};
use config::{Config, Environment, File};
use tap::Pipe;

use crate::models::AppConfig;

const PREFIX: &str = "OCEAN_CTRL";
const DEFAULT_CONFIG_PATH: &str = "config.toml";

static APP_CONFIG: OnceLock<AppConfig> = OnceLock::new();

pub fn app_config() -> AppConfig {
    APP_CONFIG
        .get()
        .expect("APP_CONFIG has to be initialized")
        .clone()
}

pub fn init() -> Result<()> {
    let config_filepath =
        std::env::var(key("CONFIG_PATH")).unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_owned());

    let settings = Config::builder()
        .add_source(config_filepath.pipe_borrow(File::with_name))
        .add_source(PREFIX.pipe(Environment::with_prefix))
        .build()
        .context("Failed to build server config")?;

    settings
        .try_deserialize::<AppConfig>()
        .context("Failed to deserialize server config")?
        .pipe(|config| {
            APP_CONFIG
                .set(config)
                .expect("Failed to set ACCESS_PASSWORD")
        });
    Ok(())
}

fn key(key: &str) -> String {
    format!("{PREFIX}_{key}")
}
