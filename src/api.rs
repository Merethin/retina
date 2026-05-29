use std::sync::Arc;

use axum::{Json, Router, extract::{Path, State}, http::StatusCode, routing::{get, post}};
use tokio::sync::{RwLock, broadcast, mpsc};

use crate::{command::Command, data::DataStorage, query::{Nation, Region, query_members, query_nation, query_region, query_regionmates}, sse::RegionEvent};
use crate::sse::start_stream;

#[derive(Clone)]
pub struct ApiState {
    pub data: Arc<RwLock<DataStorage>>,
    pub sender: mpsc::Sender<Command>,
    pub broadcast: Arc<broadcast::Sender<RegionEvent>>,
}

type ApiResult<T> = Result<T, (StatusCode, String)>;

pub async fn run_api_server(
    data: Arc<RwLock<DataStorage>>,
    sender: mpsc::Sender<Command>,
    broadcast: broadcast::Sender<RegionEvent>
) -> Result<(), std::io::Error> {
    let app = Router::new()
        .route("/members", get(world_members))
        .route("/members/{name}", get(region_members))
        .route("/region/{name}", get(region))
        .route("/regionmates/{name}", get(regionmates))
        .route("/nation/{name}", get(nation))
        .route("/sse/{events}/{view}", get(start_stream))
        .route("/bootstrap", post(bootstrap))
        .with_state(ApiState { data, sender, broadcast: Arc::new(broadcast) });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:16636")
        .await
        .unwrap();

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn world_members(
    State(state): State<ApiState>,
) -> ApiResult<Json<Vec<String>>> {
    let members = query_members(state.data, None).await.map_err(|err| {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    })?;

    Ok(Json(members))
}

async fn region_members(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Vec<String>>> {
    let members = query_members(state.data, Some(&name)).await.map_err(|err| {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    })?;

    Ok(Json(members))
}

async fn region(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Region>> {
    let region = query_region(state.data, &name).await.map_err(|err| {
        (StatusCode::NOT_FOUND, err.to_string())
    })?;

    Ok(Json(region))
}

async fn regionmates(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Region>> {
    let region = query_regionmates(state.data, &name).await.map_err(|err| {
        (StatusCode::NOT_FOUND, err.to_string())
    })?;

    Ok(Json(region))
}

async fn nation(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Nation>> {
    let nation = query_nation(state.data, &name).await.map_err(|err| {
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