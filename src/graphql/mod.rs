use std::sync::Arc;

use async_graphql::{EmptyMutation, Schema};
use tokio::sync::broadcast;

use crate::{data::GlobalData, events::{SubscriptionDetails}, graphql::{query::Query, sub::Subscription}};

mod modified;
mod nation;
pub mod query;
mod region;
mod sub;
mod world;

pub fn build_schema(
    data: Arc<GlobalData>,
    sender: broadcast::Sender<SubscriptionDetails>
) -> Schema<Query, EmptyMutation, Subscription> {
    Schema::build(Query, EmptyMutation, Subscription).data(data).data(sender).finish()
}