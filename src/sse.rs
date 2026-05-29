use std::{collections::HashSet, error::Error};

use axum::{extract::{FromRequestParts, Path, State}, http::{StatusCode, request::Parts}, response::{Sse, sse::{Event as SseEvent, KeepAlive}}};
use caramel::types::akari::Event;
use futures_util::stream::Stream;
use async_stream::try_stream;
use serde_json::{Map, Value};

use crate::{api::ApiState, query::Region};

pub type RegionEvent = (Event, Option<Region>, Option<Region>);

pub async fn start_stream(
    State(state): State<ApiState>,
    StreamExtractor(params): StreamExtractor,
) -> Sse<impl Stream<Item = Result<SseEvent, Box<dyn Error + Send + Sync>>>> {
    let mut rx = state.broadcast.subscribe();

    Sse::new(try_stream! {
        yield SseEvent::default().comment("connected");

        while let Ok(event) = rx.recv().await {
            if params.matches(&event.0) && let Some(data) = params.build_state_data(event).await {
                yield SseEvent::default().data(serde_json::to_string(&data)?);
            }
        }
    }).keep_alive(KeepAlive::new().text("keep-alive"))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum View {
    World,
    Region(String),
    Nation(String),
}

impl View {
    fn parse(s: &str) -> Result<Self, String> {
        if s == "world" {
            return Ok(View::World);
        }

        if let Some(region) = s.strip_prefix("region:") {
            return Ok(View::Region(region.to_string()));
        }

        if let Some(nation) = s.strip_prefix("nation:") {
            return Ok(View::Nation(nation.to_string()));
        }

        Err(format!("invalid view: {s}"))
    }
}

pub struct StreamParams {
    pub events: HashSet<String>,
    pub view: HashSet<View>
}

impl StreamParams {
    fn matches(&self, event: &Event) -> bool {
        if !self.events.contains(&event.category) {
            return false;
        }

        if self.view.contains(&View::World) {
            return true;
        }

        if let Some(nation) = &event.actor && self.view.contains(&View::Nation(nation.clone())) {
            return true;
        }

        if let Some(nation) = &event.receptor && self.view.contains(&View::Nation(nation.clone())) {
            return true;
        }

        if let Some(region) = &event.origin && self.view.contains(&View::Region(region.clone())) {
            return true;
        }

        if let Some(region) = &event.destination && self.view.contains(&View::Region(region.clone())) {
            return true;
        }

        false
    }

    async fn build_state_data(&self, event: RegionEvent) -> Option<Value> {
        let mut state: Map<String, Value> = Map::new();
        if let Some(data) = event.1 { state.insert(data.region.clone(), serde_json::to_value(data).unwrap()); }
        if let Some(data) = event.2 { state.insert(data.region.clone(), serde_json::to_value(data).unwrap()); }
        if state.is_empty() { return None; }

        let mut response: Map<String, Value> = Map::new();
        response.insert("event".into(), Value::Number(event.0.event.into()));
        response.insert("time".into(), Value::Number(event.0.time.into()));

        if let Some(value) = event.0.actor { response.insert("actor".into(), Value::String(value)); }
        if let Some(value) = event.0.receptor { response.insert("receptor".into(), Value::String(value)); }
        if let Some(value) = event.0.origin { response.insert("origin".into(), Value::String(value)); }
        if let Some(value) = event.0.destination { response.insert("destination".into(), Value::String(value)); }

        response.insert("category".into(), Value::String(event.0.category));
        response.insert("data".into(), Value::Array(event.0.data.into_iter().map(|v| Value::String(v)).collect()));
        response.insert("state".into(), Value::Object(state));

        Some(Value::Object(response))
    }
}

pub struct StreamExtractor(pub StreamParams);

impl<S> FromRequestParts<S> for StreamExtractor
where
    S: Send + Sync,
{
    type Rejection = (axum::http::StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let Path((events, view)) = Path::<(String, String)>::from_request_parts(parts, state).await.map_err(|e| {
            (StatusCode::BAD_REQUEST, e.to_string())
        })?;

        let events = events.split("+").map(|v| v.to_string()).collect();
        let view = view.split("+").flat_map(View::parse).collect();

        Ok(Self(StreamParams { events, view }))
    }
}