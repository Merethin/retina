use std::collections::HashMap;
use std::error::Error;

use caramel::types::akari::Event;
use log::{info, error};
use sqlx::{PgPool, PgTransaction};
use tokio::sync::mpsc::{Sender, Receiver, channel};

use crate::cache::EntityCache;
use crate::bootstrap::{bootstrap_tables_from_initial_data, build_nation_update_times, fetch_data_dump_and_events};
use crate::actions::*;

pub enum Command {
    Event(Event),
    Bootstrap
}

async fn execute_event(
    event: &Event, 
    cache: &mut EntityCache,
    tx: &mut PgTransaction<'_>
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    match event.category.as_str() {
        "wadmit" => handle_admit(tx, cache, event).await,
        "wresign" => handle_resign(tx, cache, event).await,
        "wkick" | "ncte" => handle_cte(tx, cache, event).await,
        "wendo" => handle_endo(tx, event).await,
        "wunendo" => handle_remove_endo(tx, event).await,
        "move" => handle_move(tx, cache, event).await,
        "rupdate" => handle_update(tx, cache, event).await,
        "ndel" => handle_new_delegate(tx, event).await,
        "rdel" => handle_replaced_delegate(tx, event).await,
        "ldel" => handle_lost_delegate(tx, event).await,
        _ => Ok(())
    }
}

pub async fn start_command_worker(pool: PgPool) -> Sender<Command> {
    let (tx, rx) = channel(1000);

    tokio::spawn(async move {
        worker(rx, pool).await.unwrap_or_else(|err| {
            error!("Error in command processing worker: {err}");
        });
    });

    tx
}

async fn worker(
    mut rx: Receiver<Command>,
    pool: PgPool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut last_event_id = 0i64;
    let mut cache = EntityCache::empty();

    while let Some(command) = rx.recv().await {
        match command {
            Command::Event(event) => run_event(event, &mut rx, &pool, &mut cache, &mut last_event_id).await?,
            Command::Bootstrap => run_bootstrap(&pool, &mut cache, &mut last_event_id).await?,
        }
    }

    Ok(())
}

async fn run_event(
    event: Event,
    rx: &mut Receiver<Command>,
    pool: &PgPool,
    cache: &mut EntityCache,
    last_event_id: &mut i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Already processed through bootstrap
    if event.event <= *last_event_id {
        return Ok(());
    }

    let mut tx = pool.begin().await?;

    execute_event(&event, cache, &mut tx).await.ok();
    *last_event_id = event.event;

    let mut pending_events = 1;

    // If there are events queued immediately, process them in batches
    while let Some(command) = rx.try_recv().ok() {
        match command {
            Command::Event(event) => {
                execute_event(&event, cache, &mut tx).await.ok();
                *last_event_id = event.event;
                pending_events += 1;

                if pending_events > 50 {
                    break;
                }
            },
            Command::Bootstrap => {
                tx.commit().await?; // Flush events before bootstrap

                run_bootstrap(pool, cache, last_event_id).await?;
                return Ok(());
            }
        }
    }

    tx.commit().await?;

    Ok(())
}

fn should_execute_event(
    event: &Event,
    nation_update_times: &HashMap<String, i64>,
) -> bool {
    let nation = match event.category.as_str() {
        "wadmit" | "wresign" | "move" => event.actor.as_ref().unwrap(),
        // Endos are tracked by target so we return receptor
        _ => event.receptor.as_ref().unwrap()
    };

    if let Some(time) = nation_update_times.get(nation) {
        return event.time >= *time as u64;
    }

    false
}

async fn run_bootstrap(
    pool: &PgPool,
    cache: &mut EntityCache,
    last_event_id: &mut i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Ok((nations, update_events, subseq_events)) = fetch_data_dump_and_events(pool).await {
        info!("Bootstrapping state from data dump ({} nations) + {} update events + {} subsequent events", nations.len(), update_events.len(), subseq_events.len());

        let nation_update_times = build_nation_update_times(pool).await?;

        bootstrap_tables_from_initial_data(pool, nations).await?;

        *cache = EntityCache::load(pool).await?;

        info!("Bootstrapped initial tables, filling in update & subsequent events");

        for chunk in update_events.chunks(1000) {
            let mut tx = pool.begin().await?;

            for event in chunk {
                if should_execute_event(event, &nation_update_times) {
                    execute_event(&event, cache, &mut tx).await.ok();
                }

                *last_event_id = event.event;
            }

            tx.commit().await?;
        }

        info!("Filled in update events, moving on to subsequent events");

        for chunk in subseq_events.chunks(2000) {
            let mut tx = pool.begin().await?;

            for event in chunk {
                execute_event(&event, cache, &mut tx).await.ok();
                *last_event_id = event.event;
            }

            tx.commit().await?;
        }

        info!("Bootstrap complete, final event: {}", *last_event_id);
    }

    Ok(())
}