mod config;
mod models;
mod state;

use std::fs;

use anyhow::{Context, Result};
use askama::Template;
use askama_web::WebTemplate;
use axum::{
    Form, Router,
    extract::{MatchedPath, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Redirect},
    routing::get,
};
use const_format::formatcp;
use futures::future::OptionFuture;
use tap::{Pipe, Tap};
use tower_sessions::{Expiry, MemoryStore, Session, SessionManagerLayer};
use tracing::debug;

use crate::{models::IndexForm, state::SharedIndexState};
use crate::{
    models::{AppConfig, GameServerState, LoginForm},
    state::IndexState,
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();
    config::init()?;

    let config = {
        const CONFIG_FILEPATH: &str = "config.toml";
        fs::read_to_string("config.toml")
            .context(formatcp!("Failed to read {CONFIG_FILEPATH}"))?
            .pipe_borrow(toml::from_str::<AppConfig>)
            .context(formatcp!("Failed to parse {CONFIG_FILEPATH}"))?
    };

    let app = Router::new()
        .merge(Router::new().route("/login", get(login_page).post(login_action)))
        .merge(
            Router::new()
                .route("/", get(index_page).post(index_action))
                .with_state(
                    SharedIndexState::builder()
                        .build()
                        .tap(|s| s.load_servers(&config.servers)),
                )
                .layer(middleware::from_fn(check_access)),
        )
        .layer(
            MemoryStore::default()
                .pipe(SessionManagerLayer::new)
                .with_secure(false)
                .with_expiry(Expiry::OnSessionEnd),
        );

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

async fn check_access(
    session: Session,
    request: Request<axum::body::Body>,
    next: Next,
) -> impl IntoResponse {
    request
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str() != "/login")
        .unwrap_or(true)
        .then(async || {
            session
                .get::<bool>("is_authenticated")
                .await
                .unwrap_or_default()
                .unwrap_or(false)
        })
        .pipe(OptionFuture::from)
        .await
        .unwrap_or(true)
        .then(async || next.run(request).await)
        .pipe(OptionFuture::from)
        .await
        .unwrap_or_else(|| Redirect::to("/login").into_response())
}

async fn login_page() -> impl IntoResponse {
    LoginTemplate {
        is_not_failed: true,
    }
    .tap(|t| debug!("Render login state {:?}", t))
}

async fn login_action(session: Session, Form(form): Form<LoginForm>) -> impl IntoResponse {
    if form.password == config::access_password() {
        session
            .insert("is_authenticated", true)
            .await
            .inspect_err(|e| debug!("Failed to set session: {e}"))
            .ok();
        Redirect::to("/")
            .tap(|_| debug!("Redirect to root"))
            .into_response()
    } else {
        LoginTemplate {
            is_not_failed: false,
        }
        .tap(|t| debug!("Render login state {:?}", t))
        .into_response()
    }
}

#[derive(Debug, Clone, Template, WebTemplate)]
#[template(path = "login.html")]
struct LoginTemplate {
    is_not_failed: bool,
}

async fn index_page(State(state): State<SharedIndexState>) -> impl IntoResponse {
    state.update().await;
    state.state().pipe(IndexTemplate::from)
}

async fn index_action(
    State(state): State<SharedIndexState>,
    Form(form): Form<IndexForm>,
) -> impl IntoResponse {
    state.update().await;
    match form.action.as_deref() {
        Some("start_server") => {
            if let Some(id) = form.server_id {
                state.start_server(&id)
            }
        }
        Some("stop_server") => {
            if let Some(id) = form.server_id {
                state.stop_server(&id)
            }
        }
        Some(unknown) => debug!("Unknown action: {}", unknown),
        None => (),
    };
    state.state().pipe(IndexTemplate::from)
}

#[derive(Debug, Clone, Template, WebTemplate)]
#[template(path = "index.html")]
struct IndexTemplate {
    date: String,
    servers: Vec<GameServerState>,
    has_server_online: bool,
    in_server_start_cooldown: bool,
    has_output: bool,
    output: String,
}

impl From<IndexState> for IndexTemplate {
    fn from(state: IndexState) -> Self {
        let servers = state.servers();
        Self {
            date: state.date,
            servers,
            has_server_online: state.has_server_online,
            in_server_start_cooldown: state.in_server_start_cooldown,
            has_output: !&(state.output).is_empty(),
            output: state.output,
        }
    }
}
