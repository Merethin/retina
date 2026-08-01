use std::sync::Arc;
use caramel::types::akari::Event;
use ordermap::OrderSet;
use tokio::sync::RwLock;
use crate::{data::{DataStorage, NationData}, events::{NationCreateEvent, NationDeleteEvent, RegionChangeEvent, SubscriptionEvent::{self, NationCreate, NationDelete, RegionChange}}, graphql::{Nation, Region}};

pub async fn handle_found(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let rkey = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    let data = NationData {
        name,
        region: rkey,
        is_wa: false,
        delegate: None,
        lastupdate: 0,
        endorsements: OrderSet::new()
    };

    let nation = Nation::from_nation_data(&data);
    w.nations.insert(name, data);

    let region = w.regions.entry(rkey).or_default();
    region.nations.insert(name);
    let region = Region::from_region_data(rkey, region);

    Ok(vec![
        RegionChange(RegionChangeEvent { name: event.origin.clone().unwrap(), region }),
        NationCreate(NationCreateEvent { nation }),
    ])
}

pub async fn handle_cte(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());

    let Some(nation) = w.nations.remove(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    if nation.is_wa {
        w.wa_nations.remove(&name);
    }

    let mut events = vec![
        NationDelete(NationDeleteEvent { nation: Nation::from_nation_data(&nation) })
    ];

    if let Some(delegate) = nation.delegate {
        let region = w.regions.entry(delegate).or_default();
        region.delegate = None;

        let region = Region::from_region_data(delegate, region);

        if delegate != nation.region {
            events.push(RegionChange(RegionChangeEvent { 
                name: w.interner.resolve(delegate).unwrap().to_string(), region
            }));
        }
    }

    if let Some(region) = w.regions.get_mut(&nation.region) {
        region.nations.remove(&name);

        events.push(RegionChange(RegionChangeEvent { 
            name: event.origin.clone().unwrap(), 
            region: Region::from_region_data(nation.region, region)
        }));
    }

    Ok(events)
}