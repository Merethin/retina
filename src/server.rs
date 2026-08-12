use std::sync::Arc;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQL, GraphQLSubscription};
use axum::{Router, extract::State, http::StatusCode, response::{self, IntoResponse}, routing::{get, post}};
use tokio::sync::{broadcast, mpsc};

use crate::{data::GlobalData, events::SubscriptionDetails, graphql, worker::Command};

#[derive(Clone)]
pub struct ApiState {
    pub sender: mpsc::Sender<Command>,
}

type ApiResult<T> = Result<T, (StatusCode, String)>;

pub async fn run_server(
    data: Arc<GlobalData>,
    sender: mpsc::Sender<Command>,
    broadcast: broadcast::Sender<SubscriptionDetails>
) -> Result<(), std::io::Error> {
    let schema = graphql::build_schema(data, broadcast);

    let app = Router::new()
        .route("/", get(graphiql).post_service(GraphQL::new(schema.clone())))
        .route_service("/sub", GraphQLSubscription::new(schema))
        .route("/bootstrap", post(bootstrap))
        .with_state(ApiState { sender });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:16636")
        .await
        .unwrap();

    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn graphiql() -> impl IntoResponse {
    response::Html(GraphiQLSource::build().endpoint("/").subscription_endpoint("/sub").finish())
}

async fn bootstrap(
    State(state): State<ApiState>,
) -> ApiResult<String> {
    state.sender.send(Command::Bootstrap).await.map_err(|err| {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    })?;

    Ok("success".into())
}