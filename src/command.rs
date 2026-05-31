use std::error::Error;
use std::sync::Arc;

use caramel::types::akari::Event;
use log::{info, error};
use sqlx::PgPool;
use tokio::sync::{RwLock, broadcast};
use tokio::sync::mpsc::{Sender, Receiver, channel};

use crate::bootstrap::{bootstrap_tables_from_initial_data, fetch_data_dump_and_events, query_update_times};
use crate::actions::*;
use crate::data::DataStorage;
use crate::query::query_region;
use crate::sse::RegionEvent;

pub enum Command {
    Event(Event),
    Bootstrap
}

async fn execute_event(
    event: &Event, 
    data: Arc<RwLock<DataStorage>>,
) -> anyhow::Result<bool> {
    match event.category.as_str() {
        "wadmit" => handle_admit(data, event).await,
        "wresign" | "wkick" => handle_resign(data, event).await,
        "nfound" | "nrefound" => handle_found(data, event).await,
        "ncte" => handle_cte(data, event).await,
        "wendo" => handle_endo(data, event).await,
        "wunendo" => handle_remove_endo(data, event).await,
        "move" => handle_move(data, event).await,
        "rupdate" => handle_update(data, event).await,
        "ndel" => handle_new_delegate(data, event).await,
        "rdel" => handle_replaced_delegate(data, event).await,
        "ldel" => handle_lost_delegate(data, event).await,
        _ => Ok(false)
    }
}

pub async fn start_command_worker(
    pool: PgPool, 
    broadcast: broadcast::Sender<RegionEvent>,
    data: Arc<RwLock<DataStorage>>,
) -> Sender<Command> {
    let (tx, rx) = channel(1000);

    tokio::spawn(async move {
        worker(rx, broadcast, pool, data).await.unwrap_or_else(|err| {
            error!("Error in command processing worker: {err}");
        });
    });

    tx
}

async fn worker(
    mut rx: Receiver<Command>,
    broadcast: broadcast::Sender<RegionEvent>,
    pool: PgPool,
    data: Arc<RwLock<DataStorage>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut last_event_id = 0i64;

    while let Some(command) = rx.recv().await {
        match command {
            Command::Event(event) => run_event(event, &broadcast, data.clone(), &mut last_event_id).await?,
            Command::Bootstrap => run_bootstrap(&pool, data.clone(), &mut last_event_id).await?,
        }
    }

    Ok(())
}

async fn run_event(
    event: Event,
    broadcast: &broadcast::Sender<RegionEvent>,
    data: Arc<RwLock<DataStorage>>,
    last_event_id: &mut i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Already processed through bootstrap
    if event.event <= *last_event_id {
        return Ok(());
    }

    let handled = execute_event(&event, data.clone()).await.unwrap_or(false);
    *last_event_id = event.event;

    if handled && broadcast.receiver_count() > 0 && event.category != "rupdate" {
        broadcast.send((
            event.clone(),
            if let Some(region) = &event.origin {
                query_region(data.clone(), region).await.ok()
            } else { None },
            if let Some(region) = &event.destination {
                query_region(data.clone(), region).await.ok()
            } else { None }
        )).ok();
    }

    Ok(())
}

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

async fn run_bootstrap(
    pool: &PgPool,
    data: Arc<RwLock<DataStorage>>,
    last_event_id: &mut i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Ok((nations, update_events, subseq_events)) = fetch_data_dump_and_events(pool).await {
        info!("Bootstrapping state from data dump ({} nations) + {} update events + {} subsequent events", nations.len(), update_events.len(), subseq_events.len());

        let update_times = query_update_times(pool).await?;

        *data.write().await = bootstrap_tables_from_initial_data(nations, update_times).await?;

        info!("Bootstrapped initial state, filling in update & subsequent events");

        for event in update_events {
            if should_execute_event(&event, data.clone()).await {
                execute_event(&event, data.clone()).await.ok();
            }

            *last_event_id = event.event;
        }

        info!("Filled in update events, moving on to subsequent events");

        for event in subseq_events {
            execute_event(&event, data.clone()).await.ok();
            *last_event_id = event.event;
        }

        info!("Bootstrap complete, final event: {}", *last_event_id);
    }

    Ok(())
}