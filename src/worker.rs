use std::error::Error;
use std::sync::Arc;

use caramel::types::akari::Event;
use log::error;
use sqlx::PgPool;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{Sender, Receiver, channel};

use crate::bootstrap::run_bootstrap;
use crate::actions::execute_event;
use crate::data::GlobalData;
use crate::events::{SubscriptionDetails, SubscriptionEvent};

pub enum Command {
    Event(Event),
    Bootstrap
}

pub async fn start_command_worker(
    pool: PgPool, 
    broadcast: broadcast::Sender<SubscriptionDetails>,
    data: Arc<GlobalData>,
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
    broadcast: broadcast::Sender<SubscriptionDetails>,
    pool: PgPool,
    data: Arc<GlobalData>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut last_event_id = 0i64;

    while let Some(command) = rx.recv().await {
        match command {
            Command::Event(event) => run_event(event, &broadcast, data.clone(), &mut last_event_id).await?,
            Command::Bootstrap => run_bootstrap(&pool, &broadcast, data.clone(), &mut last_event_id).await?,
        }
    }

    Ok(())
}

async fn run_event(
    event: Event,
    broadcast: &broadcast::Sender<SubscriptionDetails>,
    data: Arc<GlobalData>,
    last_event_id: &mut i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Already processed through bootstrap
    if event.event <= *last_event_id {
        return Ok(());
    }

    let last_snapshot = data.last_snapshot.read().await.clone();
    let mut snapshot = last_snapshot.modify(event.event);
    let results = {
        let mut interner = data.interner.write().await;
        execute_event(&event, &mut interner, &mut snapshot).await.unwrap_or(vec![])
    };
    
    *last_event_id = event.event;

    let arc = Arc::new(snapshot);
    *data.last_snapshot.write().await = arc.clone();

    broadcast.send(SubscriptionDetails { 
        event: SubscriptionEvent::SiteEvent(event), 
        before: last_snapshot.clone(), after: arc.clone() 
    }).ok();

    for event in results {
        broadcast.send(SubscriptionDetails { event, before: last_snapshot.clone(), after: arc.clone() }).ok();
    }

    Ok(())
}