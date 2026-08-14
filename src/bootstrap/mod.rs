pub mod query;
mod update;

use std::{collections::{HashMap, HashSet}, error::Error, sync::{Arc, atomic::Ordering}};
use caramel::types::akari::Event;
use log::info;
use sqlx::PgPool;
use string_interner::symbol::SymbolU32;
use tokio::sync::broadcast;
use futures_util::StreamExt;

use crate::{actions::{execute_event, insert_nation_if_missing}, akari::KEYS, bootstrap::{query::{query_data_dump_nation_state, query_last_major_from_data_dump, query_subsequent_bootstrap_events, query_update_bootstrap_events, query_update_times}, update::invalidate_endorsements}, data::{GlobalData, Interner, NationData, Snapshot}, events::{SubscriptionDetails, SubscriptionEvent::Bootstrap}};

pub use query::Nation;

async fn should_execute_event(
    event: &Event,
    data: Arc<GlobalData>,
    snapshot: &Snapshot
) -> bool {
    let interner = data.interner.read().await;

    if event.category == "nfound" || event.category == "nrefound" { return true; }

    let Some(index) = interner.get(match event.category.as_str() {
        "wadmit" | "wresign" | "wkick" | "move" => event.actor.as_ref().unwrap(),
        // Endos are tracked by target so we return receptor
        _ => event.receptor.as_ref().unwrap()
    }) else {
        return false
    };

    let Some(nation) = snapshot.nations.get(&index) else {
        return false;
    };

    return event.time >= nation.lastupdate;
}

pub async fn run_preliminary_bootstrap(
    pool: &PgPool,
    update_events: &Vec<Event>,
    update_start: i64,
    data: Arc<GlobalData>,
    last_event_id: &mut i64,
) -> Result<Snapshot, Box<dyn Error + Send + Sync>> {
    let update_times = query_update_times(pool).await?;

    let (saved, mut snapshot, existing) = {
        let generation = data.generation_counter.fetch_add(1, Ordering::SeqCst);
        let last_snapshot = data.last_snapshot.read().await.clone();
        let mut interner = data.interner.write().await;

        let saved = save_nonupdaters(update_start, &update_events, &interner, &last_snapshot);
        let mut snapshot = Snapshot::start_generation(generation);
        bootstrap_storage_from_initial_data(pool, update_times, &mut interner, &mut snapshot).await?;
        let existing = save_extant_nation_names(&update_events, &mut interner, &snapshot);

        (saved, snapshot, existing)
    };

    info!("Bootstrapped initial state, filling in update events");

    for event in update_events {
        if event.category == "rupdate" {
            let interner = data.interner.read().await;
            invalidate_endorsements(&event, &interner, &mut snapshot, &existing).await;
        } else if should_execute_event(&event, data.clone(), &snapshot).await {
            let mut interner = data.interner.write().await;
            execute_event(&event, &mut interner, &mut snapshot).await.ok();
        }

        *last_event_id = event.event;
    }

    info!("Filled in update events, moving on to nonupdaters");

    for nation in saved {
        let mut interner = data.interner.write().await;
        insert_nation_if_missing(&mut interner, &mut snapshot, &nation).await?;
    }

    Ok(snapshot)
}

pub async fn run_bootstrap(
    pool: &PgPool,
    broadcast: &broadcast::Sender<SubscriptionDetails>,
    data: Arc<GlobalData>,
    last_event_id: &mut i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let last_snapshot = data.last_snapshot.read().await.clone();

    if let Ok((update_start, update_end)) = query_last_major_from_data_dump(pool).await {
        let update_events = query_update_bootstrap_events(pool, update_start, update_end, KEYS.to_vec()).await?;

        info!("Bootstrapping state from data dump with {} update events", update_events.len());

        let mut snapshot = run_preliminary_bootstrap(pool, &update_events, update_start, data.clone(), last_event_id).await?;

        info!("Filled in nonupdaters, moving on to subsequent events");

        let mut subseq_events = query_subsequent_bootstrap_events(pool, update_end, KEYS.to_vec()).await?;
        while let Some(Some(event)) = subseq_events.next().await {
            let mut interner = data.interner.write().await;
            execute_event(&event, &mut interner, &mut snapshot).await.ok();
            *last_event_id = event.event;
        }

        info!("Bootstrap complete, final event: {}", *last_event_id);
        snapshot.event = *last_event_id;

        let arc = Arc::new(snapshot);
        *data.last_snapshot.write().await = arc.clone();

        broadcast.send(SubscriptionDetails { 
            event: Bootstrap, before: last_snapshot, after: arc
        }).ok();
    }

    Ok(())
}

pub async fn bootstrap_storage_from_initial_data(
    pool: &PgPool,
    update_times: HashMap<String, i64>,
    interner: &mut Interner,
    snapshot: &mut Snapshot,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    for (region, time) in &update_times {
        snapshot.regions.entry(interner.get_or_intern(region)).or_default().lastupdate = *time as u64;
    }

    let mut nations = query_data_dump_nation_state(pool);
    while let Some(Some(nation)) = nations.next().await {
        let name = interner.get_or_intern(&nation.name);
        let region = interner.get_or_intern(&nation.region);
        let endorsements = nation.endorsements.iter().map(|v| interner.get_or_intern(v)).collect();

        snapshot.nations.insert(name, NationData {
            name,
            region,
            is_wa: nation.is_wa,
            delegate: if nation.is_delegate { Some(region) } else { None },
            lastupdate: *update_times.get(&nation.region).unwrap_or(&0) as u64,
            endorsements
        });

        snapshot.regions.entry(region).or_default().nations.insert(name);

        if nation.is_delegate {
            snapshot.regions.entry(region).or_default().delegate = Some(name);
        }
    }

    snapshot.wa_nations = snapshot.nations.values().filter_map(|v| {
        if v.is_wa {
            Some(v.name)
        } else { None }
    }).collect();

    Ok(())
}

// Save a list of nations that are expected to not have updated this major update from the current snapshot.
// If after bootstrap, any of these nations are missing from the new snapshot, they will be reinserted.
pub fn save_nonupdaters(
    update_start: i64,
    update_events: &Vec<Event>,
    interner: &Interner,
    snapshot: &Snapshot
) -> Vec<Nation> {
    let mut nations: HashSet<String> = HashSet::new();

    for event in update_events {
        if event.category == "move" {
            nations.insert(event.actor.clone().unwrap());
        }
    }

    let mut result = Vec::new();

    for name in nations {
        let Some(nation) = interner.get(&name).and_then(
            |s| snapshot.nations.get(&s)
        ) else { continue; };

        if nation.lastupdate > (update_start as u64) {
            continue;
        }

        let Some(region) = interner.resolve(nation.region).map(|s| s.to_string()) else {
            continue;
        };

        let is_delegate = Some(nation.region) == nation.delegate;

        let endorsements = nation.endorsements.iter().filter_map(|v| {
            interner.resolve(*v).map(|s| s.to_string())
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

pub fn save_extant_nation_names(
    update_events: &Vec<Event>,
    interner: &mut Interner,
    snapshot: &Snapshot,
) -> HashMap<SymbolU32, i64> {
    let mut nations: HashMap<String, i64> = HashMap::new();

    for event in update_events {
        match event.category.as_str() {
            "move" | "nfound" | "nrefound" => {
                let key = event.actor.as_ref().unwrap();
                if !nations.contains_key(key) {
                    nations.insert(key.clone(), event.event);
                }
            },
            "ncte" => {
                nations.remove(event.receptor.as_ref().unwrap());
            }
            _ => {},
        }
    }

    let mut result: HashMap<SymbolU32, i64> = snapshot.nations.keys().copied().map(|s| (s, 0)).collect();

    for (name, id) in nations {
        result.insert(interner.get_or_intern(name), id);
    }

    result
}