mod bootstrap;
mod actions;
mod command;
mod events;
mod cache;
mod api;

use log::error;
use sqlx::PgPool;
use std::{error::Error, process::exit};

use caramel::log::setup_log;

use crate::{events::start_akari_worker, api::run_api_server, command::{Command, start_command_worker}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    setup_log(vec!["lapin"]);

    dotenv::dotenv().ok();

    let conn = connect_to_rabbitmq().await?;
    let channel = conn.create_channel().await?;
    let pool = connect_to_db().await;

    let sender = start_command_worker(pool.clone()).await;
    start_akari_worker(sender.clone(), channel).await.ok();

    sender.send(Command::Bootstrap).await?;

    run_api_server(pool, sender).await.unwrap_or_else(|err| {
        error!("Error in API server: {err}");
    });

    Ok(())
}

async fn connect_to_db() -> PgPool {
    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|err| {
        error!("Missing DATABASE_URL environment variable: {err}");
        exit(1);
    });

    PgPool::connect(&db_url).await.unwrap_or_else(|err| {
        error!("Error connecting to Postgres: {}", err);
        exit(1);
    })
}

async fn connect_to_rabbitmq() -> Result<lapin::Connection, Box<dyn Error>> {
    let rabbitmq_url = std::env::var("RABBITMQ_URL").unwrap_or_else(|err| {
        error!("Missing RABBITMQ_URL environment variable: {err}");
        exit(1);
    });

    Ok(lapin::Connection::connect(
        &rabbitmq_url,
        lapin::ConnectionProperties::default(),
    ).await?)
}