use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::data::DataStorage;

pub async fn handle_admit(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    nation.is_wa = true;
    w.wa_nations.insert(name);

    Ok(true)
}

pub async fn handle_resign(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    
    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    nation.is_wa = false;
    nation.endorsements.clear();
    let delegacy = std::mem::replace(&mut nation.delegate, None);

    if let Some(delegacy) = delegacy {
        w.regions.entry(delegacy).or_default().delegate = None;
    }

    w.wa_nations.retain(|v| *v != name);

    Ok(true)
}