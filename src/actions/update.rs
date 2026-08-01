use std::sync::Arc;
use caramel::types::akari::Event;
use tokio::sync::RwLock;
use crate::data::DataStorage;

pub async fn handle_update(
    data: Arc<RwLock<DataStorage>>,
    event: &Event
) -> anyhow::Result<bool> {
    let mut w = data.write().await;
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());
    let time = event.time;
    let permissible_update_time = event.time - (3 * 60 * 60);

    let Some(residents) = w.regions.get_mut(&region).map(|s| {
        s.lastupdate = time;
        s.nations.iter().copied().collect::<Vec<_>>()
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
        w.nations.get_mut(&member).map(|value| {
            value.endorsements.retain(|endorser| {
                valid_endorsers.binary_search(endorser).is_ok()
            });
        });
    }

    Ok(true)
}