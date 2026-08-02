use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::{data::DataStorage, events::{NationChangeEvent, RegionChangeEvent, RegionDeleteEvent, RegionUpdateEvent, SubscriptionEvent::{self, NationChange, RegionChange, RegionDelete, RegionUpdate}}, graphql::{Nation, Region}};

pub async fn handle_update(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let mut w = data.write().await;
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());
    let time = event.time;
    let permissible_update_time = event.time - (3 * 60 * 60);

    let mut events = vec![];

    let Some(residents) = w.regions.get_mut(&region).map(|s| {
        s.lastupdate = time;
        let nations = s.nations.iter().copied().collect::<Vec<_>>();
        let region = Region::from_region_data(region, s);

        events.push(RegionUpdate(RegionUpdateEvent {
            name: event.origin.clone().unwrap(), region: region.clone()
        }));

        events.push(RegionChange(RegionChangeEvent {
            name: event.origin.clone().unwrap(), region
        }));

        nations
    }) else {
        return Ok(vec![]);
    };

    if residents.is_empty() {
        w.regions.remove(&region);
        
        events.push(RegionDelete(RegionDeleteEvent {
            name: event.origin.clone().unwrap()
        }));
        
        return Ok(events);
    }

    let mut to_update = Vec::new();
    let mut valid_endorsers = Vec::new();

    for index in residents {
        if let Some(nation) = w.nations.get_mut(&index) {
            if nation.lastupdate < permissible_update_time {
                nation.lastupdate = time;
                if nation.is_wa { to_update.push(index); }
                else {
                    let nation = Nation::from_nation_data(nation);
                    events.push(NationChange(NationChangeEvent {
                        name: w.interner.resolve(index).unwrap().to_string(), nation
                    }));

                    continue;
                }
            }

            if nation.is_wa {
                valid_endorsers.push(nation.name);
            }
        }
    }

    if to_update.is_empty() { return Ok(events); }
    valid_endorsers.sort_unstable();

    for member in to_update {
        let name = w.interner.resolve(member).unwrap().to_string();

        w.nations.get_mut(&member).map(|value| {
            value.endorsements.retain(|endorser| {
                valid_endorsers.binary_search(endorser).is_ok()
            });

            events.push(NationChange(NationChangeEvent {
                name, nation: Nation::from_nation_data(value)
            }));
        });
    }

    Ok(events)
}