use caramel::types::akari::Event;
use crate::{data::{Interner, Snapshot}, events::SubscriptionEvent::{self, NationChange, RegionChange}};

pub async fn handle_move(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let name = interner.get_or_intern(event.actor.as_ref().unwrap());
    let origin = interner.get_or_intern(event.origin.as_ref().unwrap());
    let dest = interner.get_or_intern(event.destination.as_ref().unwrap());

    let Some(nation) = snapshot.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };
    
    nation.region = dest;

    let mut events = vec![ NationChange(name) ];

    if let Some(source) = snapshot.regions.get_mut(&origin) {
        source.nations.remove(&name);

        events.push(RegionChange(origin));
    }

    let region = snapshot.regions.entry(dest).or_default();
    region.nations.insert(name);

    events.push(RegionChange(dest));

    Ok(events)
}