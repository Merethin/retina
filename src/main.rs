mod actions;
mod akari;
mod bootstrap;
mod data;
mod events;
mod graphql;
mod replay;
mod server;
mod worker;

use clap::{Parser, Subcommand};
use log::error;
use sqlx::PgPool;
use std::{error::Error, fs::File, process::exit, sync::Arc};
use tokio::sync::broadcast;

use caramel::log::setup_log;

use crate::{akari::start_akari_worker, data::GlobalData, replay::run_replay, server::run_server, worker::{Command, start_command_worker}};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    StartServer,
    Replay {
        path: String,
        query: String
    },
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    setup_log(vec!["lapin"]);

    dotenv::dotenv().ok();

    match args.cmd {
        Commands::StartServer => main_loop().await?,
        Commands::Replay { path, query } => {
            let mut file = File::create(path)?;
            let pool = connect_to_db().await;
            let data = Arc::new(GlobalData::new());
            run_replay(&pool, data, &mut file, query).await?;
        }
    }

    Ok(())
}

async fn main_loop() -> Result<(), Box<dyn Error + Send + Sync>> {
    let conn = connect_to_rabbitmq().await?;
    let channel = conn.create_channel().await?;
    let pool = connect_to_db().await;

    let (broadcast, _rx) = broadcast::channel(100);
    drop(_rx);

    let data = Arc::new(GlobalData::new());

    let sender = start_command_worker(pool, broadcast.clone(), data.clone()).await;
    start_akari_worker(sender.clone(), channel).await.ok();

    sender.send(Command::Bootstrap).await?;

    run_server(data, sender, broadcast).await.unwrap_or_else(|err| {
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

async fn connect_to_rabbitmq() -> Result<lapin::Connection, Box<dyn Error + Send + Sync>> {
    let rabbitmq_url = std::env::var("RABBITMQ_URL").unwrap_or_else(|err| {
        error!("Missing RABBITMQ_URL environment variable: {err}");
        exit(1);
    });

    Ok(lapin::Connection::connect(
        &rabbitmq_url,
        lapin::ConnectionProperties::default(),
    ).await?)
}