use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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

    pub fn date(&self) -> String {
        self.state().date
    }

    pub async fn update(&self) {
        let state = self.state();
        let to_clear_output = state.last_output_time + state.output_shift_time < Utc::now();
        let has_server_online = state
            .servers
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
            .map(async |server: GameServer| {
                let text = server
                    .config
                    .cmd_list_server_players
                    .pipe_as_ref(utils::await_process_output)
                    .await;
                todo!("parse player list from text: {text}");
            })
            .pipe(OptionFuture::from)
            .await
            .unwrap_or(false)
    }

    pub fn load_servers(&self, servers: Vec<(GameServerConfig, GameServerState)>) {
        let servers = servers
            .into_iter()
            .map(|(config, state)| (config.id.clone(), GameServer { config, state }))
            .collect();
        self.state()
            .set_servers(servers)
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
                    if server.state.is_online {
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
                if !server.state.is_online {
                    format!("服务器 {server_id} 不在线, 拒绝关闭")
                        .tap(|_| warn!("stop server {} failed: server not online", server_id))
                } else if server.state.has_player {
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
        self.state().servers.remove(server_id).ok_or_else(|| {
            format!("不存在的的服务器 {server_id}")
                .tap(|_| warn!("server {} not exists", server_id))
        })
    }

    fn state(&self) -> IndexState {
        self.0.lock().unwrap().clone()
    }

    fn set_state(&self, state: IndexState) -> IndexState {
        self.0
            .lock()
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
                                    error!(
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

    pub fn output_shift_time(mut self, output_shift_time: TimeDelta) -> Self {
        self.output_shift_time = output_shift_time;
        self
    }

    pub fn build(self) -> SharedIndexState {
        IndexState {
            date: "NaN".to_owned(),
            servers: HashMap::new(),

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
struct IndexState {
    date: String,

    servers: HashMap<String, GameServer>,

    has_server_online: bool,
    in_server_start_cooldown: bool,

    output: String,
    output_shift_time: TimeDelta,
    last_output_time: DateTime<Utc>,
}

impl IndexState {
    fn update_date(self) -> Self {
        Self {
            date: utils::time_now_string(),
            ..self
        }
    }

    fn set_servers(self, servers: HashMap<String, GameServer>) -> Self {
        Self { servers, ..self }
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
    state: GameServerState,
}
