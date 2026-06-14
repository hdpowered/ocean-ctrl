use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::{convert, mem};

use chrono::{DateTime, Local, TimeDelta, Utc};
use futures::future::OptionFuture;
use futures::stream::{self, StreamExt};
use tap::{Pipe, Tap};
use tracing::{debug, error, info, warn};

use crate::models::{GameServerConfig, GameServerState};

#[derive(Debug, Clone)]
pub struct SharedIndexState(Arc<Mutex<IndexState>>);

impl SharedIndexState {
    pub fn builder() -> SharedIndexStateBuilder {
        SharedIndexStateBuilder::new()
    }

    pub async fn update(&self) {
        let state = self.state();
        let to_clear_output = state.last_output_time + state.output_shift_time < Utc::now();
        let has_server_online = state
            .servers_map
            .keys()
            .pipe(stream::iter)
            .any(async |server_id| self.check_server_players(server_id).await)
            .await;

        let state = state.update_date().set_has_server_online(has_server_online);
        let state = if to_clear_output {
            state.set_output("")
        } else {
            state
        };
        let state = if has_server_online {
            state.set_in_server_start_cooldown(false)
        } else {
            state
        };
        self.set_state(state);
    }

    async fn check_server_players(&self, server_id: &str) -> bool {
        server_id
            .pipe(|server_id| self.require_server(server_id))
            .ok()
            .map(async |server| {
                let player_cnt = server
                    .config
                    .cmd_list_server_players
                    .pipe_as_ref(utils::await_process_output)
                    .await
                    .pipe_as_ref(utils::match_player_count_online);

                let is_online: bool;
                let has_player: bool;

                if let Some(cnt) = player_cnt {
                    is_online = true;
                    has_player = cnt > 0;
                    info!("check: {} player in server {}", cnt, server_id)
                } else {
                    is_online = false;
                    has_player = false;
                    info!("check: server {} not online", server_id)
                };

                server
                    .state()
                    .set_is_online(is_online)
                    .set_has_player(has_player)
                    .pipe(|state| server.set_state(state));

                is_online
            })
            .pipe(OptionFuture::from)
            .await
            .unwrap_or(false)
    }

    pub fn load_servers(&self, servers: &[GameServerConfig]) {
        let servers = servers
            .iter()
            .cloned()
            .map(|config| (config.id.clone(), GameServer::new(config)))
            .collect();
        self.state()
            .set_servers_map(servers)
            .pipe(|state| self.set_state(state));
    }

    pub fn start_server(&self, server_id: &str) {
        let prev_state = self.state();
        let output = if prev_state.in_server_start_cooldown {
            "有服务器启动中, 拒绝启动"
                .to_owned()
                .tap(|_| debug!("start server {} failed: server starting", server_id))
        } else if prev_state.has_server_online {
            "存在其他服务器在线, 拒绝启动"
                .to_owned()
                .tap(|_| debug!("start server {} failed: other server online", server_id))
        } else {
            server_id
                .pipe(|server_id| self.require_server(server_id))
                .map_or_else(convert::identity, |server| {
                    if server.state().is_online {
                        format!("服务器 {server_id} 已在线").tap(|_| {
                            warn!(
                                "server {} online while should have no server online",
                                server_id,
                            )
                        })
                    } else {
                        server
                            .config
                            .cmd_start_server
                            .pipe_as_ref(utils::spawn_process);
                        format!("请求启动服务器 {}", server_id)
                            .tap(|_| info!("start server {}", server_id))
                    }
                })
        };
        prev_state
            .set_output(&output)
            .set_in_server_start_cooldown(true)
            .pipe(|state| self.set_state(state));
    }

    pub fn stop_server(&self, server_id: &str) {
        let output = server_id
            .pipe(|server_id| self.require_server(server_id))
            .map_or_else(convert::identity, |server| {
                let state = server.state();
                if !state.is_online {
                    format!("服务器 {server_id} 不在线, 拒绝关闭")
                        .tap(|_| warn!("stop server {} failed: server not online", server_id))
                } else if state.has_player {
                    format!("服务器 {server_id} 运行中, 拒绝关闭")
                        .tap(|_| warn!("stop server {} failed: has player", server_id))
                } else {
                    server
                        .config
                        .cmd_stop_server
                        .pipe_as_ref(utils::spawn_process);
                    format!("请求暂停服务器 {}", server_id)
                        .tap(|_| info!("stop server {}", server_id))
                }
            });
        self.state()
            .set_output(&output)
            .pipe(|state| self.set_state(state));
    }

    fn require_server(&self, server_id: &str) -> Result<GameServer, String> {
        self.state().servers_map.remove(server_id).ok_or_else(|| {
            format!("不存在的的服务器 {server_id}")
                .tap(|_| warn!("server {} not exists", server_id))
        })
    }

    pub fn state(&self) -> IndexState {
        self.0.lock().unwrap().clone()
    }

    fn set_state(&self, state: IndexState) -> IndexState {
        self.0
            .lock()
            .unwrap()
            .pipe_deref_mut(|g| mem::replace(g, state))
    }
}

pub struct SharedIndexStateBuilder {
    output_shift_time: TimeDelta,
}

impl SharedIndexStateBuilder {
    fn new() -> Self {
        Self {
            output_shift_time: TimeDelta::seconds(60),
        }
    }

    #[allow(dead_code)]
    pub fn output_shift_time(mut self, output_shift_time: TimeDelta) -> Self {
        self.output_shift_time = output_shift_time;
        self
    }

    pub fn build(self) -> SharedIndexState {
        IndexState {
            date: "NaN".to_owned(),
            servers_map: HashMap::new(),

            has_server_online: false,
            in_server_start_cooldown: false,
            output: String::new(),
            output_shift_time: self.output_shift_time,
            last_output_time: Utc::now(),
        }
        .pipe(Mutex::new)
        .pipe(Arc::new)
        .pipe(SharedIndexState)
    }
}

#[derive(Debug, Clone)]
pub struct IndexState {
    pub date: String,

    servers_map: HashMap<String, GameServer>,

    pub has_server_online: bool,
    pub in_server_start_cooldown: bool,

    pub output: String,
    output_shift_time: TimeDelta,
    last_output_time: DateTime<Utc>,
}

impl IndexState {
    pub fn servers(&self) -> Vec<GameServerState> {
        self.servers_map
            .values()
            .map(|server| server.state())
            .collect()
    }

    fn update_date(self) -> Self {
        Self {
            date: utils::time_now_string(),
            ..self
        }
    }

    fn set_servers_map(self, servers: HashMap<String, GameServer>) -> Self {
        Self {
            servers_map: servers,
            ..self
        }
    }

    fn set_has_server_online(self, has_server_online: bool) -> Self {
        Self {
            has_server_online,
            ..self
        }
    }

    fn set_in_server_start_cooldown(self, in_server_start_cooldown: bool) -> Self {
        Self {
            in_server_start_cooldown,
            ..self
        }
    }

    fn set_output(self, output: &str) -> Self {
        Self {
            date: utils::time_now_string(),
            output: output.to_owned(),
            last_output_time: Utc::now(),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
struct GameServer {
    config: GameServerConfig,
    state: Arc<RwLock<GameServerState>>,
}

impl GameServer {
    fn new(config: GameServerConfig) -> Self {
        let state = GameServerState::new(config.id.clone(), config.name.clone())
            .pipe(RwLock::new)
            .pipe(Arc::new);

        Self { config, state }
    }

    fn state(&self) -> GameServerState {
        self.state.read().unwrap().clone()
    }

    fn set_state(&self, state: GameServerState) -> GameServerState {
        self.state
            .write()
            .unwrap()
            .pipe_deref_mut(|g| mem::replace(g, state))
    }
}

mod utils {
    use std::ops::Not;

    use futures::future::OptionFuture;
    use tokio::process::Command;

    use super::*;

    pub fn time_now_string() -> String {
        Local::now().to_rfc3339()
    }

    pub fn match_player_count_online(list_output: &str) -> Option<usize> {
        const PATTERN: &str = r"^There are\s+(\d+)\s+of a max of\s+\d+\s+players online";

        regex::Regex::new(PATTERN)
            .inspect_err(|e| error!("failed to compile regex: {:?}", e))
            .ok()
            .and_then(|re| {
                re.captures(list_output).and_then(|cap| {
                    cap.get(1).map(|m| {
                        m.as_str()
                            .parse::<usize>()
                            .inspect_err(|e| {
                                error!(
                                    "failed to parse player count from string {:?}: {:?}",
                                    m.as_str(),
                                    e
                                )
                            })
                            .ok()
                    })?
                })
            })
    }

    pub fn spawn_process<T: AsRef<str>>(cmd: &[T]) {
        cmd.is_empty().not().then(|| {
            Command::new(cmd[0].as_ref())
                .args(cmd[1..].iter().map(AsRef::as_ref))
                .spawn()
                .inspect_err(|e| {
                    error!(
                        "failed to start process of command {:?}: {:?}",
                        cmd.iter().map(AsRef::as_ref).collect::<Vec<&str>>(),
                        e
                    )
                })
                .map(|mut child| {
                    tokio::spawn(async move { child.wait().await.inspect_err(|e| error!(?e)) })
                })
                .ok();
        });
    }

    pub async fn await_process_output<T: AsRef<str>>(cmd: &[T]) -> String {
        cmd.is_empty()
            .not()
            .then(async || {
                Command::new(cmd[0].as_ref())
                    .args(cmd[1..].iter().map(AsRef::as_ref))
                    .output()
                    .await
                    .inspect_err(|e| {
                        error!(
                            "failed to start process of command {:?}: {:?}",
                            cmd.iter().map(AsRef::as_ref).collect::<Vec<&str>>(),
                            e
                        )
                    })
                    .map(|output| {
                        if output.status.success() {
                            String::from_utf8(output.stdout).inspect_err(|e| error!(?e))
                        } else {
                            String::from_utf8(output.stderr)
                                .inspect_err(|e| error!(?e))
                                .inspect(|stderr| {
                                    info!(
                                        "process of command {:?} failed with stderr: {}",
                                        cmd.iter().map(AsRef::as_ref).collect::<Vec<&str>>(),
                                        stderr
                                    )
                                })
                        }
                        .unwrap_or_default()
                    })
                    .unwrap_or_default()
            })
            .pipe(OptionFuture::from)
            .await
            .unwrap_or_default()
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[tokio::test]
        async fn test_match_player_count_online() {
            let output = "There are 0 of a max of 20 players online";
            assert_eq!(match_player_count_online(output), Some(0));

            let output = "There are 3 of a max of 20 players online: player1, player2, player3";
            assert_eq!(match_player_count_online(output), Some(3));

            let output = "There are 10 of a max of 20 players online: ...";
            assert_eq!(match_player_count_online(output), Some(10));

            let output = "Invalid output";
            assert_eq!(match_player_count_online(output), None);
        }
    }
}
