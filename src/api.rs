use std::collections::HashMap;

use axum::{Json, Router, extract::{Path, State}, http::StatusCode, routing::{get, post}};
use sqlx::{PgPool, Postgres};
use tokio::sync::mpsc::Sender;
use serde::Serialize;

use crate::command::Command;

#[derive(Clone)]
struct ApiState {
    pool: PgPool,
    sender: Sender<Command>,
}

type ApiResult<T> = Result<T, StatusCode>;

pub async fn run_api_server(
    pool: PgPool,
    sender: Sender<Command>
) -> Result<(), std::io::Error> {
    let app = Router::new()
        .route("/members", get(world_members))
        .route("/members/{name}", get(region_members))
        .route("/delegates", get(delegates))
        .route("/region/{name}", get(region))
        .route("/regionmates/{name}", get(regionmates))
        .route("/nation/{name}", get(nation))
        .route("/bootstrap", post(bootstrap))
        .with_state(ApiState { pool, sender });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:16636")
        .await
        .unwrap();

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn world_members(
    State(state): State<ApiState>,
) -> ApiResult<Json<Vec<String>>> {
    let members: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM retina_nations"
    ).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    Ok(Json(members))
}

async fn region_members(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Vec<String>>> {
    let members: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM retina_nations WHERE region = $1"
    ).bind(name).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    Ok(Json(members))
}

#[derive(Serialize)]
struct Delegate {
    name: String,
    region: String,
    endos_received: i64,
    endos_given: i64,
}

async fn delegates(
    State(state): State<ApiState>,
) -> ApiResult<Json<Vec<Delegate>>> {
    let delegates: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, delegacy FROM retina_nations WHERE delegacy IS NOT NULL"
    ).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let names = delegates.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();

    let endos_received: HashMap<String, i64> = sqlx::query_as::<Postgres, (String, i64)>(
        "SELECT target, COUNT(*) AS count FROM retina_endorsements WHERE target = ANY($1) GROUP BY target"
    ).bind(&names).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?.into_iter().collect();

    let endos_given: HashMap<String, i64> = sqlx::query_as::<Postgres, (String, i64)>(
        "SELECT endorser, COUNT(*) AS count FROM retina_endorsements WHERE endorser = ANY($1) GROUP BY endorser"
    ).bind(&names).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?.into_iter().collect();

    let mut result: Vec<Delegate> = Vec::new();
    for (name, region) in delegates {
        let er = *endos_received.get(&name).unwrap_or(&0);
        let eg = *endos_given.get(&name).unwrap_or(&0);

        result.push(Delegate {
            name, region,
            endos_received: er,
            endos_given: eg
        });
    }

    Ok(Json(result))
}

#[derive(Serialize)]
struct Region {
    delegate: Option<String>,
    nations: Vec<RegionMember>,
}

#[derive(Serialize)]
struct RegionMember {
    name: String,
    endorsements: Vec<String>,
}

async fn region(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Region>> {
    let delegate: Option<String> = sqlx::query_scalar(
        "SELECT name FROM retina_nations WHERE delegacy = $1"
    ).bind(&name).fetch_optional(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let members: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM retina_nations WHERE region = $1"
    ).bind(&name).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let endos_received: Vec<(String, String)> = sqlx::query_as(
        "SELECT target, endorser FROM retina_endorsements WHERE target = ANY($1)"
    ).bind(&members).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for (target, endorser) in endos_received {
        grouped.entry(target).or_default().push(endorser);
    }

    let mut result = vec![];
    for name in members {
        let endorsements = grouped.get(&name).cloned().unwrap_or_default();

        result.push(RegionMember {
            name, endorsements
        });
    }

    Ok(Json(Region { delegate, nations: result }))
}

#[derive(Serialize)]
struct Regionmates {
    region: String,
    delegate: Option<String>,
    nations: Vec<RegionMember>,
}

async fn regionmates(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Regionmates>> {
    let region: String = sqlx::query_scalar(
        "SELECT region FROM retina_nations WHERE name = $1"
    ).bind(&name).fetch_one(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let delegate: Option<String> = sqlx::query_scalar(
        "SELECT name FROM retina_nations WHERE delegacy = $1"
    ).bind(&region).fetch_optional(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let members: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM retina_nations WHERE region = $1"
    ).bind(&region).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let endos_received: Vec<(String, String)> = sqlx::query_as(
        "SELECT target, endorser FROM retina_endorsements WHERE target = ANY($1)"
    ).bind(&members).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for (target, endorser) in endos_received {
        grouped.entry(target).or_default().push(endorser);
    }

    let mut result = vec![];
    for name in members {
        let endorsements = grouped.get(&region).cloned().unwrap_or_default();

        result.push(RegionMember {
            name, endorsements
        });
    }

    Ok(Json(Regionmates { region, delegate, nations: result }))
}

#[derive(Serialize)]
struct Nation {
    region: String,
    is_delegate: bool,
    endos_received: Vec<String>,
    endos_given: Vec<String>,
}

async fn nation(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Nation>> {
    let data: (String, Option<String>) = sqlx::query_as(
        "SELECT region, delegacy FROM retina_nations WHERE name = $1"
    ).bind(&name).fetch_one(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let endorsements: Vec<(String, String)> = sqlx::query_as(
        "SELECT target, endorser FROM retina_endorsements WHERE target = $1 OR endorser = $1"
    ).bind(&name).fetch_all(&state.pool).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    let mut endos_received = vec![];
    let mut endos_given = vec![];

    for (target, endorser) in endorsements {
        if target == name {
            endos_received.push(endorser);
        } else {
            endos_given.push(target);
        }
    }

    let is_delegate = Some(&data.0) == data.1.as_ref();

    Ok(Json(Nation {
        region: data.0,
        is_delegate,
        endos_received,
        endos_given
    }))
}

async fn bootstrap(
    State(state): State<ApiState>,
) -> ApiResult<Json<()>> {
    state.sender.send(Command::Bootstrap).await.map_err(
        |_| StatusCode::INTERNAL_SERVER_ERROR
    )?;

    Ok(Json(()))
}