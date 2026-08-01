pub mod query;

use std::{collections::{HashMap, HashSet}, error::Error, sync::Arc};
use caramel::types::akari::Event;
use log::info;
use sqlx::PgPool;
use tokio::sync::{RwLock, broadcast};

use crate::{actions::{execute_event, insert_nation_if_missing}, bootstrap::query::{fetch_data_dump_and_events, query_update_times}, data::{DataStorage, NationData}, events::{BootstrapEvent, SubscriptionEvent::{self, Bootstrap}}};

pub use query::Nation;

async fn should_execute_event(
    event: &Event,
    data: Arc<RwLock<DataStorage>>,
) -> bool {
    let r = data.read().await;

    let Some(index) = r.interner.get(match event.category.as_str() {
        "wadmit" | "wresign" | "wkick" | "move" | "nfound" | "nrefound" => event.actor.as_ref().unwrap(),
        // Endos are tracked by target so we return receptor
        _ => event.receptor.as_ref().unwrap()
    }) else {
        return false
    };

    let Some(nation) = r.nations.get(&index) else {
        if event.category == "nfound" || event.category == "nrefound" { return true; }
        return false;
    };

    return event.time >= nation.lastupdate;
}

pub async fn run_bootstrap(
    pool: &PgPool,
    broadcast: &broadcast::Sender<SubscriptionEvent>,
    data: Arc<RwLock<DataStorage>>,
    last_event_id: &mut i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Ok((nations, update_events, subseq_events, update_start)) = fetch_data_dump_and_events(pool).await {
        info!("Bootstrapping state from data dump ({} nations) + {} update events + {} subsequent events", nations.len(), update_events.len(), subseq_events.len());

        let update_times = query_update_times(pool).await?;

        let saved = {
            let r = data.read().await;
            save_nonupdaters(update_start, &update_events, &r)
        };

        *data.write().await = bootstrap_storage_from_initial_data(nations, update_times)?;

        info!("Bootstrapped initial state, filling in update & subsequent events");

        for event in update_events {
            if should_execute_event(&event, data.clone()).await {
                execute_event(&event, data.clone()).await.ok();
            }

            *last_event_id = event.event;
        }

        info!("Filled in update events, moving on to nonupdaters");

        for nation in saved {
            insert_nation_if_missing(data.clone(), &nation).await?;
        }

        info!("Filled in nonupdaters, moving on to subsequent events");

        for event in subseq_events {
            execute_event(&event, data.clone()).await.ok();
            *last_event_id = event.event;
        }

        info!("Bootstrap complete, final event: {}", *last_event_id);

        broadcast.send(Bootstrap(BootstrapEvent { last_id: *last_event_id })).ok();
    }

    Ok(())
}

pub fn bootstrap_storage_from_initial_data(
    nations: Vec<Nation>,
    update_times: HashMap<String, i64>,
) -> Result<DataStorage, Box<dyn Error + Send + Sync>> {
    let mut data = DataStorage::new();

    data.nations.reserve(nations.len());

    for (region, time) in &update_times {
        data.regions.entry(data.interner.get_or_intern(region)).or_default().lastupdate = *time as u64;
    }

    for nation in &nations {
        let name = data.interner.get_or_intern(&nation.name);
        let region = data.interner.get_or_intern(&nation.region);
        let endorsements = nation.endorsements.iter().map(|v| data.interner.get_or_intern(v)).collect();

        data.nations.insert(name, NationData {
            name,
            region,
            is_wa: nation.is_wa,
            delegate: if nation.is_delegate { Some(region) } else { None },
            lastupdate: *update_times.get(&nation.region).unwrap_or(&0) as u64,
            endorsements
        });

        data.regions.entry(region).or_default().nations.insert(name);

        if nation.is_delegate {
            data.regions.entry(region).or_default().delegate = Some(name);
        }
    }

    data.wa_nations = nations.iter().filter_map(|v| {
        if v.is_wa {
            Some(data.interner.get_or_intern(&v.name))
        } else { None }
    }).collect();

    Ok(data)
}

// Save a list of nations that are expected to not have updated this major update from the current snapshot.
// If after bootstrap, any of these nations are missing from the new snapshot, they will be reinserted.
pub fn save_nonupdaters(
    update_start: i64,
    update_events: &Vec<Event>,
    storage: &DataStorage
) -> Vec<Nation> {
    let mut nations: HashSet<String> = HashSet::new();

    for event in update_events {
        match event.category.as_str() {
            "move" | "nfound" | "nrefound" => nations.insert(event.actor.clone().unwrap()),
            _ => false,
        };
    }

    let mut result = Vec::new();

    for name in nations {
        let Some(nation) = storage.interner.get(&name).and_then(
            |s| storage.nations.get(&s)
        ) else { continue; };

        if nation.lastupdate > (update_start as u64) {
            continue;
        }

        let Some(region) = storage.interner.resolve(nation.region).map(|s| s.to_string()) else {
            continue;
        };

        let is_delegate = Some(nation.region) == nation.delegate;

        let endorsements = nation.endorsements.iter().filter_map(|v| {
            storage.interner.resolve(*v).map(|s| s.to_string())
        }).collect();

        result.push(Nation { 
            name: name.clone(), 
            is_wa: nation.is_wa,
            is_delegate, 
            region, 
            endorsements
        });
    }

    result
}