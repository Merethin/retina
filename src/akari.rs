use std::error::Error;

use tokio::sync::mpsc::Sender;
use lazy_static::lazy_static;

use crate::worker::Command;
use caramel::akari::{create_consumer, consume};

lazy_static! {
    pub static ref KEYS: Vec<&'static str> = vec![
        "wadmit", "wresign", "wkick", "ncte", 
        "nfound", "nrefound",
        "wendo", "wunendo", 
        "move",
        "rupdate", 
        "ndel", "rdel", "ldel"
    ];
}

pub async fn start_akari_worker(
    sender: Sender<Command>,
    channel: lapin::Channel
) -> Result<(), Box<dyn Error>> {
    let mut consumer = create_consumer(&channel, "akari_events", Some(KEYS.to_vec())).await?;

    tokio::spawn(async move {
        while let Some(event) = consume(&mut consumer).await {
            sender.send(Command::Event(event)).await.ok();
        }
    });

    Ok(())
}