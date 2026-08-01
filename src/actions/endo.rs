use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::data::DataStorage;

pub async fn handle_endo(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let endorser = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let target = w.interner.get_or_intern(event.receptor.as_ref().unwrap());

    if let Some(nation) = w.nations.get_mut(&target) {
        nation.endorsements.insert(endorser);
    }

    Ok(true)
}

pub async fn handle_remove_endo(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let endorser = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let target = w.interner.get_or_intern(event.receptor.as_ref().unwrap());

    if let Some(nation) = w.nations.get_mut(&target) {
        nation.endorsements.remove(&endorser);
    }

    Ok(true)
}