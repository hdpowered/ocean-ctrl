use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub servers: Vec<GameServerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GameServerConfig {
    pub id: String,
    pub name: String,
    pub cmd_list_server_players: Vec<String>,
    pub cmd_start_server: Vec<String>,
    pub cmd_stop_server: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GameServerState {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub is_online: bool,
    #[serde(default)]
    pub has_player: bool,
}

impl GameServerState {
    pub fn new(id: String, name: String) -> Self {
        Self {
            id,
            name,
            is_online: false,
            has_player: false,
        }
    }

    pub fn set_is_online(self, is_online: bool) -> Self {
        Self { is_online, ..self }
    }

    pub fn set_has_player(self, has_player: bool) -> Self {
        Self { has_player, ..self }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoginRequest {
    pub password: String,
}
