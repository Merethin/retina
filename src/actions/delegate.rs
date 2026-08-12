use caramel::types::akari::Event;
use crate::{data::{Interner, Snapshot}, events::SubscriptionEvent::{self, NationChange, RegionChange}};

pub async fn handle_new_delegate(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let name = interner.get_or_intern(event.receptor.as_ref().unwrap());
    let rkey = interner.get_or_intern(event.origin.as_ref().unwrap());

    let Some(nation) = snapshot.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = Some(rkey);

    let region = snapshot.regions.entry(rkey).or_default();
    region.delegate = Some(name);

    Ok(vec![ RegionChange(rkey), NationChange(name) ])
}

pub async fn handle_replaced_delegate(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let name = interner.get_or_intern(event.receptor.as_ref().unwrap());
    let rkey = interner.get_or_intern(event.origin.as_ref().unwrap());
    let old_del = interner.get_or_intern(event.data.get(0).unwrap());

    let Some(nation) = snapshot.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = Some(rkey);

    let region = snapshot.regions.entry(rkey).or_default();
    region.delegate = Some(name);

    let Some(old_nation) = snapshot.nations.get_mut(&old_del) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    old_nation.delegate = None;

    Ok(vec![ RegionChange(rkey), NationChange(name), NationChange(old_del) ])
}

pub async fn handle_lost_delegate(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let name = interner.get_or_intern(event.receptor.as_ref().unwrap());
    let rkey = interner.get_or_intern(event.origin.as_ref().unwrap());

    let Some(nation) = snapshot.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = None;

    let region = snapshot.regions.entry(rkey).or_default();
    region.delegate = Some(name);

    Ok(vec![ RegionChange(rkey), NationChange(name) ])
}