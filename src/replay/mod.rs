use std::{error::Error, fs::File, sync::Arc, time::Instant};

use async_graphql::{EmptyMutation, EmptySubscription, Response, Schema};
use caramel::types::akari::Event;
use log::{error, info, warn};
use sqlx::PgPool;
use csv::{Writer, WriterBuilder};

use crate::{actions::execute_event, bootstrap::{query::fetch_data_dump_and_events, run_preliminary_bootstrap}, data::GlobalData};

use crate::graphql::query::Query;

fn write_replay_line(
    writer: &mut Writer<&mut File>,
    event: &Event,
    response: &Response
) -> Result<(), Box<dyn Error + Send + Sync>> {
    Ok(writer.write_record(&[
        event.event.to_string(), 
        event.time.to_string(),
        event.category.clone(),
        serde_json::to_string(&response.data)?
    ])?)
}

pub async fn run_replay(
    pool: &PgPool,
    data: Arc<GlobalData>,
    file: &mut File,
    query: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut last_event_id = 0i64;

    let schema = Schema::build(Query, EmptyMutation, EmptySubscription).data(data.clone()).finish();
    let mut writer = WriterBuilder::new().has_headers(false).from_writer(file);

    let prelim_start = Instant::now();

    if let Ok((nations, update_events, subseq_events, update_start)) = fetch_data_dump_and_events(pool).await {
        info!("Replaying state from data dump ({} nations) + {} preliminary events + {} subsequent events", nations.len(), update_events.len(), subseq_events.len());

        let mut snapshot = run_preliminary_bootstrap(pool, nations, &update_events, update_start, data.clone(), &mut last_event_id).await?;

        info!("Preliminary bootstrap complete, final event: {} (took {}s)", last_event_id, prelim_start.elapsed().as_secs());
        snapshot.event = last_event_id;

        let replay_start = Instant::now();

        let mut snapshot = Arc::new(snapshot);
        *data.last_snapshot.write().await = snapshot.clone();

        let mut last_response = schema.execute(&query).await;
        let last_event = update_events.last().expect("No update events found");
        let mock_event = Event {
            event: last_event.event,
            time: last_event.time,
            category: "init".into(),
            actor: None,
            receptor: None,
            origin: None,
            destination: None,
            data: vec![]
        };

        if last_response.is_err() {
            warn!("Initial query failed to execute");
            for error in last_response.errors {
                error!("{}", error.message);
            }

            return Ok(());
        }

        write_replay_line(&mut writer, &mock_event, &last_response)?;

        let mut replay_counter = 1;

        for event in &subseq_events {
            let mut new_snapshot = snapshot.modify(event.event);
            let _ = {
                let mut interner = data.interner.write().await;
                execute_event(&event, &mut interner, &mut new_snapshot).await.unwrap_or(vec![])
            };

            snapshot = Arc::new(new_snapshot);
            *data.last_snapshot.write().await = snapshot.clone();

            let response = schema.execute(&query).await;
            if last_response.data != response.data {
                write_replay_line(&mut writer, event, &response)?;
                last_response = response;
                last_event_id = event.event;
                replay_counter += 1;
            }
        }

        info!("Replay complete with {} entries saved, final event: {} (took {}s)", replay_counter, last_event_id, replay_start.elapsed().as_secs());
    }

    Ok(())
}