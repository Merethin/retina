use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::{data::DataStorage, events::{DelegateChangeEvent, NationChangeEvent, RegionChangeEvent, SubscriptionEvent::{self, DelegateChange, NationChange, RegionChange}}, graphql::{Nation, Region}};

pub async fn handle_new_delegate(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());
    let rkey = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = Some(rkey);
    let nation = Nation::from_nation_data(nation);

    let region = w.regions.entry(rkey).or_default();
    region.delegate = Some(name);

    let region = Region::from_region_data(rkey, region);

    Ok(vec![
        DelegateChange(DelegateChangeEvent { name: event.origin.clone().unwrap(), region: region.clone() }),
        RegionChange(RegionChangeEvent { name: event.origin.clone().unwrap(), region }),
        NationChange(NationChangeEvent { name: event.receptor.clone().unwrap(), nation })
    ])
}

pub async fn handle_replaced_delegate(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());
    let rkey = w.interner.get_or_intern(event.origin.as_ref().unwrap());
    let old_del = w.interner.get_or_intern(event.data.get(0).unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = Some(rkey);
    let nation = Nation::from_nation_data(nation);

    let region = w.regions.entry(rkey).or_default();
    region.delegate = Some(name);

    let region = Region::from_region_data(rkey, region);

    let Some(old_nation) = w.nations.get_mut(&old_del) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    old_nation.delegate = None;

    Ok(vec![
        DelegateChange(DelegateChangeEvent { name: event.origin.clone().unwrap(), region: region.clone() }),
        RegionChange(RegionChangeEvent { name: event.origin.clone().unwrap(), region }),
        NationChange(NationChangeEvent { name: event.receptor.clone().unwrap(), nation }),
        NationChange(NationChangeEvent { name: event.data[0].clone(), nation: Nation::from_nation_data(old_nation) })
    ])
}

pub async fn handle_lost_delegate(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());
    let rkey = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = None;
    let nation = Nation::from_nation_data(nation);

    let region = w.regions.entry(rkey).or_default();
    region.delegate = Some(name);
    let region = Region::from_region_data(rkey, region);

    Ok(vec![
        DelegateChange(DelegateChangeEvent { name: event.origin.clone().unwrap(), region: region.clone() }),
        RegionChange(RegionChangeEvent { name: event.origin.clone().unwrap(), region }),
        NationChange(NationChangeEvent { name: event.receptor.clone().unwrap(), nation })
    ])
}