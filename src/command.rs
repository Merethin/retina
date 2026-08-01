use std::error::Error;
use std::sync::Arc;

use caramel::types::akari::Event;
use log::error;
use sqlx::PgPool;
use tokio::sync::{RwLock, broadcast};
use tokio::sync::mpsc::{Sender, Receiver, channel};

use crate::bootstrap::run_bootstrap;
use crate::actions::execute_event;
use crate::data::DataStorage;

pub enum Command {
    Event(Event),
    Bootstrap
}

pub async fn start_command_worker(
    pool: PgPool, 
    broadcast: broadcast::Sender<()>,
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
    broadcast: broadcast::Sender<()>,
    pool: PgPool,
    data: Arc<RwLock<DataStorage>>,
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
    broadcast: &broadcast::Sender<()>,
    data: Arc<RwLock<DataStorage>>,
    last_event_id: &mut i64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Already processed through bootstrap
    if event.event <= *last_event_id {
        return Ok(());
    }

    let handled = execute_event(&event, data.clone()).await.unwrap_or(false);
    *last_event_id = event.event;

    // FIXME: Handle event broadcast through GraphQL

    Ok(())
}