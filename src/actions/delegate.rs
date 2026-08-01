use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::data::DataStorage;

pub async fn handle_new_delegate(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = Some(region);

    w.regions.entry(region).or_default().delegate = Some(name);

    Ok(true)
}

pub async fn handle_replaced_delegate(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());
    let old_del = w.interner.get_or_intern(event.data.get(0).unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = Some(region);

    w.regions.entry(region).or_default().delegate = Some(name);

    if let Some(nation) = w.nations.get_mut(&old_del) {
        nation.delegate = None;
    } else {
        return Err(anyhow::Error::msg("Not found"))
    };

    Ok(true)
}

pub async fn handle_lost_delegate(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = None;
    w.regions.entry(region).or_default().delegate = None;

    Ok(true)
}