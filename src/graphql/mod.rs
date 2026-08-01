use std::sync::Arc;

use async_graphql::{EmptyMutation, Schema};
use tokio::sync::{RwLock, broadcast};

use crate::{data::DataStorage, events::SubscriptionEvent, graphql::{query::Query, sub::Subscription}};

mod nation;
mod query;
mod region;
mod sub;
mod world;

pub use nation::Nation;
pub use region::Region;

pub fn build_schema(
    data: Arc<RwLock<DataStorage>>,
    sender: broadcast::Sender<SubscriptionEvent>
) -> Schema<Query, EmptyMutation, Subscription> {
    Schema::build(Query, EmptyMutation, Subscription).data(data).data(sender).finish()
}