use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::{data::DataStorage, events::{NationChangeEvent, RegionChangeEvent, SubscriptionEvent::{self, NationChange, RegionChange, WAChange}, WAChangeEvent}, graphql::{Nation, Region}};

pub async fn handle_admit(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    nation.is_wa = true;
    let rkey = nation.region;
    let nation = Nation::from_nation_data(nation);

    w.wa_nations.insert(name);

    let Some(region) = w.regions.get(&rkey) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    Ok(vec![
        RegionChange(RegionChangeEvent { name: event.origin.clone().unwrap(), region: Region::from_region_data(rkey, region) }),
        NationChange(NationChangeEvent { name: event.actor.clone().unwrap(), nation: nation.clone() }),
        WAChange(WAChangeEvent { name: event.actor.clone().unwrap(), nation: nation })
    ])
}

pub async fn handle_resign(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    
    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    nation.is_wa = false;
    nation.endorsements.clear();
    let delegacy = std::mem::replace(&mut nation.delegate, None);
    let rkey = nation.region;
    let nation = Nation::from_nation_data(nation);

    let mut events = vec![
        NationChange(NationChangeEvent { name: event.actor.clone().unwrap(), nation: nation.clone() }),
        WAChange(WAChangeEvent { name: event.actor.clone().unwrap(), nation: nation })
    ];

    if let Some(delegacy) = delegacy {
        let region = w.regions.entry(delegacy).or_default();
        region.delegate = None;

        let region = Region::from_region_data(delegacy, region);

        if delegacy != rkey {
            events.push(RegionChange(RegionChangeEvent { 
                name: w.interner.resolve(delegacy).unwrap().to_string(), region
            }));
        }
    }

    w.wa_nations.remove(&name);

    let Some(region) = w.regions.get(&rkey) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    events.push(RegionChange(RegionChangeEvent { name: event.origin.clone().unwrap(), region: Region::from_region_data(rkey, region) }));

    Ok(events)
}