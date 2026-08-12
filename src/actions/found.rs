use caramel::types::akari::Event;
use im::OrdSet;
use crate::{data::{Interner, NationData, Snapshot}, events::SubscriptionEvent::{self, NationChange, RegionChange}};

pub async fn handle_found(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let name = interner.get_or_intern(event.actor.as_ref().unwrap());
    let rkey = interner.get_or_intern(event.origin.as_ref().unwrap());

    let data = NationData {
        name,
        region: rkey,
        is_wa: false,
        delegate: None,
        lastupdate: 0,
        endorsements: OrdSet::new()
    };

    snapshot.nations.insert(name, data);

    let region = snapshot.regions.entry(rkey).or_default();
    region.nations.insert(name);

    Ok(vec![ RegionChange(rkey), NationChange(name) ])
}

pub async fn handle_cte(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let name = interner.get_or_intern(event.receptor.as_ref().unwrap());

    let Some(nation) = snapshot.nations.remove(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    if nation.is_wa {
        snapshot.wa_nations.remove(&name);
    }

    let mut events = vec![ NationChange(name) ];

    if let Some(delegacy) = nation.delegate {
        let region = snapshot.regions.entry(delegacy).or_default();
        region.delegate = None;

        if delegacy != nation.region {
            events.push(RegionChange(delegacy));
        }
    }

    if let Some(region) = snapshot.regions.get_mut(&nation.region) {
        region.nations.remove(&name);

        events.push(RegionChange(nation.region));
    }

    Ok(events)
}