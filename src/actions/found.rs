use std::sync::Arc;
use caramel::types::akari::Event;
use ordermap::OrderSet;
use tokio::sync::RwLock;
use crate::data::{DataStorage, NationData};

pub async fn handle_found(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    w.nations.insert(name, NationData {
        name,
        region,
        is_wa: false,
        delegate: None,
        lastupdate: 0,
        endorsements: OrderSet::new()
    });

    w.regions.entry(region).or_default().nations.insert(name);

    Ok(true)
}

pub async fn handle_cte(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());

    let Some(nation) = w.nations.remove(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    if nation.is_wa {
        w.wa_nations.retain(|v| *v != name);
    }

    if let Some(delegate) = nation.delegate {
        w.regions.entry(delegate).or_default().delegate = None;
    }

    if let Some(region) = w.regions.get_mut(&nation.region) {
        region.nations.retain(|v| *v != name);
    }

    Ok(true)
}