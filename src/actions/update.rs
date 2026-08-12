use caramel::types::akari::Event;
use crate::{data::{Interner, Snapshot}, events::SubscriptionEvent::{self, NationChange, RegionChange}};

pub async fn handle_update(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let region = interner.get_or_intern(event.origin.as_ref().unwrap());
    let time = event.time;
    let permissible_update_time = event.time - (3 * 60 * 60);

    let mut events = vec![];

    let Some(residents) = snapshot.regions.get_mut(&region).map(|s| {
        s.lastupdate = time;
        let nations = s.nations.iter().copied().collect::<Vec<_>>();

        events.push(RegionChange(region));

        nations
    }) else {
        return Ok(vec![]);
    };

    if residents.is_empty() {
        snapshot.regions.remove(&region);
        
        return Ok(events);
    }

    let mut to_update = Vec::new();
    let mut valid_endorsers = Vec::new();

    for index in residents {
        if let Some(nation) = snapshot.nations.get_mut(&index) {
            if nation.lastupdate < permissible_update_time {
                nation.lastupdate = time;
                if nation.is_wa { to_update.push(index); }
                else {
                    events.push(NationChange(index));

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
        snapshot.nations.get_mut(&member).map(|value| {
            value.endorsements = value.endorsements.iter().filter(|endorser| {
                valid_endorsers.binary_search(endorser).is_ok()
            }).cloned().collect();

            events.push(NationChange(member));
        });
    }

    Ok(events)
}