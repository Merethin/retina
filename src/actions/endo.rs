use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::{data::DataStorage, events::{EndoChangeEvent, NationChangeEvent, RegionChangeEvent, SubscriptionEvent::{self, EndoChange, NationChange, RegionChange}}, graphql::{Nation, Region}};

pub async fn handle_endo(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let endorser = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let target = w.interner.get_or_intern(event.receptor.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&target) else {
        return Ok(vec![]);
    };

    nation.endorsements.insert(endorser);
    let rkey = nation.region;
    let nation = Nation::from_nation_data(nation);

    let Some(region) = w.regions.get(&rkey) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    Ok(vec![
        RegionChange(RegionChangeEvent { name: event.origin.clone().unwrap(), region: Region::from_region_data(rkey, region) }),
        NationChange(NationChangeEvent { name: event.receptor.clone().unwrap(), nation: nation.clone() }),
        EndoChange(EndoChangeEvent { name: event.receptor.clone().unwrap(), nation: nation })
    ])
}

pub async fn handle_remove_endo(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let endorser = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let target = w.interner.get_or_intern(event.receptor.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&target) else {
        return Ok(vec![]);
    };

    nation.endorsements.remove(&endorser);
    let rkey = nation.region;
    let nation = Nation::from_nation_data(nation);

    let Some(region) = w.regions.get(&rkey) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    Ok(vec![
        RegionChange(RegionChangeEvent { name: event.origin.clone().unwrap(), region: Region::from_region_data(rkey, region) }),
        NationChange(NationChangeEvent { name: event.receptor.clone().unwrap(), nation: nation.clone() }),
        EndoChange(EndoChangeEvent { name: event.receptor.clone().unwrap(), nation: nation })
    ])
}