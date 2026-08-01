mod delegate;
mod endo;
mod found;
mod insert;
mod r#move;
mod update;
mod wa;

use std::sync::Arc;
use tokio::sync::RwLock;
use caramel::types::akari::Event;
use crate::{data::DataStorage, events::SubscriptionEvent};

use delegate::{handle_new_delegate, handle_replaced_delegate, handle_lost_delegate};
use endo::{handle_endo, handle_remove_endo};
use found::{handle_found, handle_cte};
use r#move::handle_move;
use update::handle_update;
use wa::{handle_admit, handle_resign};

pub use insert::insert_nation_if_missing;

pub async fn execute_event(
    event: &Event, 
    data: Arc<RwLock<DataStorage>>,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    match event.category.as_str() {
        "wadmit" => handle_admit(data, event).await,
        "wresign" | "wkick" => handle_resign(data, event).await,
        "nfound" | "nrefound" => handle_found(data, event).await,
        "ncte" => handle_cte(data, event).await,
        "wendo" => handle_endo(data, event).await,
        "wunendo" => handle_remove_endo(data, event).await,
        "move" => handle_move(data, event).await,
        "rupdate" => handle_update(data, event).await,
        "ndel" => handle_new_delegate(data, event).await,
        "rdel" => handle_replaced_delegate(data, event).await,
        "ldel" => handle_lost_delegate(data, event).await,
        _ => Ok(vec![])
    }
}