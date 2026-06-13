use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub servers: Vec<GameServerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct GameServerConfig {
    pub id: String,
    pub name: String,
    pub cmd_list_server_players: Vec<String>,
    pub cmd_start_server: Vec<String>,
    pub cmd_stop_server: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct GameServerState {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub is_online: bool,
    #[serde(default)]
    pub has_player: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoginRequest {
    pub password: String,
}
