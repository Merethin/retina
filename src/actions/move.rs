use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::data::DataStorage;

pub async fn handle_move(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let origin = w.interner.get_or_intern(event.origin.as_ref().unwrap());
    let dest = w.interner.get_or_intern(event.destination.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };
    
    nation.region = dest;

    if let Some(set) = w.regions.get_mut(&origin) {
        set.nations.retain(|v| *v != name);
    }

    w.regions.entry(dest).or_default().nations.insert(name);

    Ok(true)
}