use std::sync::Arc;

use axum::{Json, Router, extract::{Path, State}, http::StatusCode, routing::{get, post}};
use caramel::types::akari::Event;
use sqlx::PgPool;
use tokio::sync::{mpsc, broadcast};

use crate::{command::Command, query::{Delegate, Nation, Region, query_delegates, query_members, query_nation, query_region, query_regionmates}};
use crate::sse::start_stream;

#[derive(Clone)]
pub struct ApiState {
    pub pool: PgPool,
    pub sender: mpsc::Sender<Command>,
    pub broadcast: Arc<broadcast::Sender<Event>>,
}

type ApiResult<T> = Result<T, (StatusCode, String)>;

pub async fn run_api_server(
    pool: PgPool,
    sender: mpsc::Sender<Command>
) -> Result<(), std::io::Error> {
    let (tx, _) = broadcast::channel(100);

    let app = Router::new()
        .route("/members", get(world_members))
        .route("/members/{name}", get(region_members))
        .route("/delegates", get(delegates))
        .route("/region/{name}", get(region))
        .route("/regionmates/{name}", get(regionmates))
        .route("/nation/{name}", get(nation))
        .route("/sse/{events}/{view}/{output}", get(start_stream))
        .route("/bootstrap", post(bootstrap))
        .with_state(ApiState { pool, sender, broadcast: Arc::new(tx) });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:16636")
        .await
        .unwrap();

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn world_members(
    State(state): State<ApiState>,
) -> ApiResult<Json<Vec<String>>> {
    let members = query_members(&state.pool, None).await.map_err(|err| {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    })?;

    Ok(Json(members))
}

async fn region_members(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Vec<String>>> {
    let members = query_members(&state.pool, Some(&name)).await.map_err(|err| {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    })?;

    Ok(Json(members))
}

async fn delegates(
    State(state): State<ApiState>,
) -> ApiResult<Json<Vec<Delegate>>> {
    let delegates = query_delegates(&state.pool).await.map_err(|err| {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    })?;

    Ok(Json(delegates))
}

async fn region(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Region>> {
    let region = query_region(&state.pool, &name).await.map_err(|err| {
        (StatusCode::NOT_FOUND, err.to_string())
    })?;

    Ok(Json(region))
}

async fn regionmates(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Region>> {
    let region = query_regionmates(&state.pool, &name).await.map_err(|err| {
        (StatusCode::NOT_FOUND, err.to_string())
    })?;

    Ok(Json(region))
}

async fn nation(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Nation>> {
    let nation = query_nation(&state.pool, &name).await.map_err(|err| {
        (StatusCode::NOT_FOUND, err.to_string())
    })?;

    Ok(Json(nation))
}

async fn bootstrap(
    State(state): State<ApiState>,
) -> ApiResult<String> {
    state.sender.send(Command::Bootstrap).await.map_err(|err| {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    })?;

    Ok("success".into())
}