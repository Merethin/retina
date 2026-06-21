use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::{bootstrap::Nation, data::{DataStorage, NationData}};

pub async fn insert_nation_if_missing(
    data: Arc<RwLock<DataStorage>>,
    nation: &Nation
) -> anyhow::Result<bool> {
    let mut w = data.write().await;

    let name = w.interner.get_or_intern(&nation.name);

    if w.nations.contains_key(&name) {
        return Ok(false);
    }

    let region = w.interner.get_or_intern(&nation.region);

    w.nations.insert(name, NationData {
        name,
        region,
        is_wa: nation.is_wa,
        delegate: if nation.is_delegate { Some(region) } else { None },
        lastupdate: 0
    });

    w.regions.entry(region).or_default().push(name);

    if nation.is_wa {
        let endorsements = nation.endorsements.iter().map(|v| w.interner.get_or_intern(v)).collect();
        w.endorsements.insert(name, endorsements);
        w.wa_nations.push(name);

        if nation.is_delegate {
            w.delegates.insert(region, name);
        }
    }

    Ok(true)
}

pub async fn handle_admit(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    nation.is_wa = true;
    w.wa_nations.push(name);

    Ok(true)
}

pub async fn handle_resign(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    
    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    nation.is_wa = false;
    let delegacy = std::mem::replace(&mut nation.delegate, None);

    if let Some(delegacy) = delegacy {
        w.delegates.remove(&delegacy);
    }

    w.wa_nations.retain(|v| *v != name);
    w.endorsements.get_mut(&name).map(|value| {
        value.clear();
    });

    Ok(true)
}

pub async fn handle_found(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    w.nations.insert(name, NationData {
        name,
        region,
        is_wa: false,
        delegate: None,
        lastupdate: 0
    });

    w.regions.entry(region).or_default().push(name);

    Ok(true)
}

pub async fn handle_cte(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());

    let Some(nation) = w.nations.remove(&name) else {
        return Err(anyhow::Error::msg("Not found"))
    };

    if nation.is_wa {
        w.wa_nations.retain(|v| *v != name);
    }

    if let Some(delegate) = nation.delegate {
        w.delegates.remove(&delegate);
    }

    if let Some(region) = w.regions.get_mut(&nation.region) {
        region.retain(|v| *v != name);
    }

    w.endorsements.get_mut(&name).map(|value| {
        value.clear();
    });

    Ok(true)
}

pub async fn handle_endo(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let endorser = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let target = w.interner.get_or_intern(event.receptor.as_ref().unwrap());

    w.endorsements.entry(target).or_default().push(endorser);

    Ok(true)
}

pub async fn handle_remove_endo(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let endorser = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let target = w.interner.get_or_intern(event.receptor.as_ref().unwrap());

    if let Some(set) = w.endorsements.get_mut(&target) {
        set.retain(|v| *v != endorser);
    }

    Ok(true)
}

pub async fn handle_move(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.actor.as_ref().unwrap());
    let origin = w.interner.get_or_intern(event.origin.as_ref().unwrap());
    let dest = w.interner.get_or_intern(event.destination.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };
    
    nation.region = dest;

    if let Some(set) = w.regions.get_mut(&origin) {
        set.retain(|v| *v != name);
    }

    w.regions.entry(dest).or_default().push(name);

    Ok(true)
}

pub async fn handle_update(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());
    let time = event.time;
    let permissible_update_time = event.time - (3 * 60 * 60);

    let Some(residents) = w.regions.get_mut(&region).map(|s| {
        s.sort_unstable();
        s.dedup();
        s.iter().copied().collect::<Vec<_>>()
    }) else {
        return Ok(false);
    };

    let mut to_update = Vec::new();
    let mut valid_endorsers = Vec::new();

    for index in residents {
        if let Some(nation) = w.nations.get_mut(&index) {
            if nation.lastupdate < permissible_update_time {
                nation.lastupdate = time;
                if nation.is_wa { to_update.push(index); }
            }

            if nation.is_wa {
                valid_endorsers.push(nation.name);
            }
        }
    }

    if to_update.is_empty() { return Ok(false); }
    valid_endorsers.sort_unstable();

    for member in to_update {
        w.endorsements.get_mut(&member).map(|value| {
            value.retain(|endorser| {
                valid_endorsers.binary_search(endorser).is_ok()
            });
            
            value.sort_unstable();
            value.dedup();
        });
    }

    Ok(true)
}

pub async fn handle_new_delegate(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = Some(region);

    if let Some(old_delegate) = w.delegates.insert(region, name) {
        if let Some(nation) = w.nations.get_mut(&old_delegate) {
            nation.delegate = None;
        } else {
            return Err(anyhow::Error::msg("Not found"))
        };
    }

    Ok(true)
}

pub async fn handle_replaced_delegate(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());
    let old_del = w.interner.get_or_intern(event.data.get(0).unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = Some(region);

    let resync = if let Some(old_delegate) = w.delegates.insert(region, name) {
        if let Some(nation) = w.nations.get_mut(&old_delegate) {
            nation.delegate = None;
        } else {
            return Err(anyhow::Error::msg("Not found"))
        };

        old_delegate != old_del
    } else {
        true
    };

    if resync {
        if let Some(nation) = w.nations.get_mut(&old_del) {
            nation.delegate = None;
        } else {
            return Err(anyhow::Error::msg("Not found"))
        };
    }

    Ok(true)
}

pub async fn handle_lost_delegate(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let name = w.interner.get_or_intern(event.receptor.as_ref().unwrap());
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    let Some(nation) = w.nations.get_mut(&name) else {
        return Err(anyhow::Error::msg("Not found"));
    };

    nation.delegate = None;

    if let Some(old_delegate) = w.delegates.remove(&region) && old_delegate != name {
        // Mismatch
        if let Some(nation) = w.nations.get_mut(&old_delegate) {
            nation.delegate = None;
        } else {
            return Err(anyhow::Error::msg("Not found"))
        };
    }

    Ok(true)
}