mod delegate;
mod endo;
mod found;
mod insert;
mod r#move;
mod update;
mod wa;

use caramel::types::akari::Event;
use crate::{data::{Interner, Snapshot}, events::SubscriptionEvent};

use delegate::{handle_new_delegate, handle_replaced_delegate, handle_lost_delegate};
use endo::{handle_endo, handle_remove_endo};
use found::{handle_found, handle_cte};
use r#move::handle_move;
use update::handle_update;
use wa::{handle_admit, handle_resign};

pub use insert::insert_nation_if_missing;

pub async fn execute_event(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    match event.category.as_str() {
        "wadmit" => handle_admit(event, interner, snapshot).await,
        "wresign" | "wkick" => handle_resign(event, interner, snapshot).await,
        "nfound" | "nrefound" => handle_found(event, interner, snapshot).await,
        "ncte" => handle_cte(event, interner, snapshot).await,
        "wendo" => handle_endo(event, interner, snapshot).await,
        "wunendo" => handle_remove_endo(event, interner, snapshot).await,
        "move" => handle_move(event, interner, snapshot).await,
        "rupdate" => handle_update(event, interner, snapshot).await,
        "ndel" => handle_new_delegate(event, interner, snapshot).await,
        "rdel" => handle_replaced_delegate(event, interner, snapshot).await,
        "ldel" => handle_lost_delegate(event, interner, snapshot).await,
        _ => Ok(vec![])
    }
}