use std::sync::Arc;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::GraphQL;
use axum::{Router, extract::State, http::StatusCode, response::{self, IntoResponse}, routing::{get, post}};
use tokio::sync::{RwLock, broadcast, mpsc};

use crate::{command::Command, data::DataStorage, graphql};

#[derive(Clone)]
pub struct ApiState {
    pub data: Arc<RwLock<DataStorage>>,
    pub sender: mpsc::Sender<Command>,
    pub broadcast: Arc<broadcast::Sender<()>>,
}

type ApiResult<T> = Result<T, (StatusCode, String)>;

pub async fn run_server(
    data: Arc<RwLock<DataStorage>>,
    sender: mpsc::Sender<Command>,
    broadcast: broadcast::Sender<()>
) -> Result<(), std::io::Error> {
    let schema = graphql::build_schema(data.clone());

    let app = Router::new()
        .route("/", get(graphiql).post_service(GraphQL::new(schema)))
        .route("/bootstrap", post(bootstrap))
        .with_state(ApiState { data, sender, broadcast: Arc::new(broadcast) });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:16636")
        .await
        .unwrap();

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn graphiql() -> impl IntoResponse {
    response::Html(GraphiQLSource::build().endpoint("/").finish())
}

async fn bootstrap(
    State(state): State<ApiState>,
) -> ApiResult<String> {
    state.sender.send(Command::Bootstrap).await.map_err(|err| {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    })?;

    Ok("success".into())
}