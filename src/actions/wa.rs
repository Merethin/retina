use caramel::types::akari::Event;
use crate::{data::{Interner, Snapshot}, events::SubscriptionEvent::{self, NationChange, RegionChange}};

pub async fn handle_admit(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let name = interner.get_or_intern(event.actor.as_ref().unwrap());

    let Some(nation) = snapshot.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    nation.is_wa = true;
    let rkey = nation.region;

    snapshot.wa_nations.insert(name);

    Ok(vec![ RegionChange(rkey), NationChange(name) ])
}

pub async fn handle_resign(
    event: &Event,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> anyhow::Result<Vec<SubscriptionEvent>> {
    let name = interner.get_or_intern(event.actor.as_ref().unwrap());
    
    let Some(nation) = snapshot.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    nation.is_wa = false;
    nation.endorsements.clear();
    let delegacy = std::mem::replace(&mut nation.delegate, None);
    let rkey = nation.region;

    let mut events = vec![
        NationChange(name),
    ];

    if let Some(delegacy) = delegacy {
        let region = snapshot.regions.entry(delegacy).or_default();
        region.delegate = None;

        if delegacy != rkey {
            events.push(RegionChange(delegacy));
        }
    }

    snapshot.wa_nations.remove(&name);

    events.push(RegionChange(rkey));

    Ok(events)
}