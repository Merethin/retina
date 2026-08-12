use caramel::types::akari::Event;
use crate::{data::{Interner, Snapshot}, events::SubscriptionEvent::{self, NationChange, RegionChange}};

pub async fn handle_endo(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let endorser = interner.get_or_intern(event.actor.as_ref().unwrap());
    let target = interner.get_or_intern(event.receptor.as_ref().unwrap());

    let Some(nation) = snapshot.nations.get_mut(&target) else {
        return Ok(vec![]);
    };

    nation.endorsements.insert(endorser);
    let rkey = nation.region;

    Ok(vec![ RegionChange(rkey), NationChange(target) ])
}

pub async fn handle_remove_endo(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let endorser = interner.get_or_intern(event.actor.as_ref().unwrap());
    let target = interner.get_or_intern(event.receptor.as_ref().unwrap());

    let Some(nation) = snapshot.nations.get_mut(&target) else {
        return Ok(vec![]);
    };

    nation.endorsements.remove(&endorser);
    let rkey = nation.region;

    Ok(vec![ RegionChange(rkey), NationChange(target) ])
}