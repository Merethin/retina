use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::{data::DataStorage, events::{NationChangeEvent, NationMoveEvent, RegionChangeEvent, SubscriptionEvent::{self, NationChange, NationMove, RegionChange}}, graphql::{Nation, Region}};

pub async fn handle_move(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let origin = w.interner.get_or_intern(event.origin.as_ref().unwrap());
    let dest = w.interner.get_or_intern(event.destination.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };
    
    nation.region = dest;

    let mut events = vec![
        NationMove(NationMoveEvent { name: event.actor.clone().unwrap(), nation: Nation::from_nation_data(nation) }),
        NationChange(NationChangeEvent { name: event.actor.clone().unwrap(), nation: Nation::from_nation_data(nation) })
    ];

    if let Some(source) = w.regions.get_mut(&origin) {
        source.nations.remove(&name);

        events.push(RegionChange(RegionChangeEvent {
            name: event.origin.clone().unwrap(),
            region: Region::from_region_data(origin, source)
        }));
    }

    let region = w.regions.entry(dest).or_default();
    region.nations.insert(name);

    events.push(RegionChange(RegionChangeEvent {
        name: event.destination.clone().unwrap(),
        region: Region::from_region_data(dest, region)
    }));

    Ok(events)
}