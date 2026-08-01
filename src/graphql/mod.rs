use std::sync::Arc;

use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use tokio::sync::RwLock;

use crate::{data::DataStorage, graphql::query::Query};

mod nation;
mod query;
mod region;
mod world;

pub fn build_schema(
    data: Arc<RwLock<DataStorage>>
) -> Schema<Query, EmptyMutation, EmptySubscription> {
    Schema::build(Query, EmptyMutation, EmptySubscription).data(data).finish()
}