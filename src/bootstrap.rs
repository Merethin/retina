use std::{collections::HashMap, error::Error, io::Write};
use sqlx::{PgPool, QueryBuilder, Row};
use serde::{Serialize, Deserialize};

use caramel::types::akari::Event;

use crate::events::KEYS;

#[derive(Debug, Serialize, Deserialize)]
pub struct Nation {
    pub name: String,
    pub is_delegate: bool,
    pub region: String,
    pub endorsements: Vec<String>,
}

pub async fn fetch_data_dump_and_events(
    pool: &PgPool
) -> Result<(Vec<Nation>, Vec<Event>, Vec<Event>), Box<dyn Error + Send + Sync>> {
    let (update_start, update_end) = query_last_major_from_data_dump(pool).await?;
    let nations = query_data_dump_nation_state(pool).await?;
    let update_events = query_update_bootstrap_events(pool, update_start, update_end, KEYS.to_vec()).await?;
    let subseq_events = query_subsequent_bootstrap_events(pool, update_end, KEYS.to_vec()).await?;

    Ok((nations, update_events, subseq_events))
}

pub async fn bootstrap_tables_from_initial_data(
    pool: &PgPool,
    nations: Vec<Nation>
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut tx = pool.begin().await?;

    sqlx::query("TRUNCATE retina_nations, retina_endorsements").execute(&mut *tx).await?;

    let mut qb = QueryBuilder::new(
        "INSERT INTO retina_nations (name, delegacy, region) "
    );

    qb.push_values(&nations, |mut b, nation| {
        b.push_bind(&nation.name)
         .push_bind(if nation.is_delegate { Some(&nation.region) } else { None })
         .push_bind(&nation.region);
    });

    qb.build().execute(&mut *tx).await?;

    let mut copy = tx.copy_in_raw(
        "COPY retina_endorsements (endorser, target) FROM STDIN"
    ).await?;

    let mut buf = Vec::with_capacity(64);

    for nation in &nations {
        for e in &nation.endorsements {
            buf.clear();
            write!(&mut buf, "{}\t{}\n", e, nation.name)?;
            copy.send(buf.as_slice()).await?;
        }
    }

    copy.finish().await?;
    tx.commit().await?;

    Ok(())
}

async fn query_last_major_from_data_dump(
    pool: &PgPool
) -> Result<(i64, i64), Box<dyn Error + Send + Sync>> {
    let update_start: i64 = sqlx::query_scalar(
        "SELECT lastmajorupdate FROM regions_dump ORDER BY lastmajorupdate ASC LIMIT 1"
    ).fetch_one(pool).await?;

    let update_end: i64 = sqlx::query_scalar(
        "SELECT lastmajorupdate FROM regions_dump ORDER BY lastmajorupdate DESC LIMIT 1"
    ).fetch_one(pool).await?;

    Ok((update_start, update_end))
}

async fn query_update_bootstrap_events(
    pool: &PgPool,
    update_start: i64,
    update_end: i64,
    types: Vec<&str>,
) -> Result<Vec<Event>, Box<dyn Error + Send + Sync>> {
    let start: i64 = sqlx::query_scalar(
        "SELECT event FROM akari_events WHERE category = 'rupdate' AND time >= $1 ORDER BY time ASC, event ASC LIMIT 1"
    ).bind(update_start).fetch_one(pool).await?;

    let end: i64 = sqlx::query_scalar(
        "SELECT event FROM akari_events WHERE category = 'rfeature' AND time >= $1 ORDER BY time ASC, event ASC LIMIT 1"
    ).bind(update_end).fetch_one(pool).await?;

    let types = types.into_iter().filter(|&t| t != "rupdate").collect::<Vec<_>>();

    let result = sqlx::query(
        "SELECT * FROM akari_events WHERE category = ANY($1) AND event BETWEEN $2 AND $3 ORDER BY event ASC"
    ).bind(&types).bind(start).bind(end).fetch_all(pool).await?;

    Ok(result.into_iter().map(|row| {
        Event {
            event: row.get("event"),
            time: row.get::<i64, &str>("time") as u64,
            actor: row.try_get("actor").ok(),
            receptor: row.try_get("receptor").ok(),
            origin: row.try_get("origin").ok(),
            destination: row.try_get("destination").ok(),
            data: row.try_get("data").unwrap_or(vec![]),
            category: row.get("category")
        }
    }).collect())
}

async fn query_subsequent_bootstrap_events(
    pool: &PgPool,
    update_end: i64,
    types: Vec<&str>,
) -> Result<Vec<Event>, Box<dyn Error + Send + Sync>> {
    let start: i64 = sqlx::query_scalar(
        "SELECT event FROM akari_events WHERE category = 'rfeature' AND time >= $1 ORDER BY time ASC, event ASC LIMIT 1"
    ).bind(update_end).fetch_one(pool).await?;

    let result = sqlx::query(
        "SELECT * FROM akari_events WHERE category = ANY($1) AND event > $2 ORDER BY event ASC"
    ).bind(&types).bind(start).fetch_all(pool).await?;

    Ok(result.into_iter().map(|row| {
        Event {
            event: row.get("event"),
            time: row.get::<i64, &str>("time") as u64,
            actor: row.try_get("actor").ok(),
            receptor: row.try_get("receptor").ok(),
            origin: row.try_get("origin").ok(),
            destination: row.try_get("destination").ok(),
            data: row.try_get("data").unwrap_or(vec![]),
            category: row.get("category")
        }
    }).collect())
}

async fn query_data_dump_nation_state(
    pool: &PgPool,
) -> Result<Vec<Nation>, Box<dyn Error + Send + Sync>> {
    let result = sqlx::query(
        "SELECT canon_name, is_delegate, region, endorsements FROM nations_dump WHERE is_wa = TRUE"
    ).fetch_all(pool).await?;

    Ok(result.into_iter().map(|row| {
        Nation {
            name: row.get(0),
            is_delegate: row.get(1),
            region: row.get(2),
            endorsements: row.get(3),
        }
    }).collect())
}

async fn query_update_times(
    pool: &PgPool,
) -> Result<HashMap<String, i64>, Box<dyn Error + Send + Sync>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT canon_name, lastmajorupdate FROM regions_dump"
    ).fetch_all(pool).await?;

    Ok(rows.into_iter().collect())
}

pub async fn build_nation_update_times(
    pool: &PgPool,
) -> Result<HashMap<String, i64>, Box<dyn Error + Send + Sync>> {
    let region_update_times = query_update_times(pool).await?;

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT canon_name, region FROM nations_dump"
    ).fetch_all(pool).await?;

    let mut result = HashMap::new();

    for (nation, region) in rows {
        result.insert(nation, *region_update_times.get(&region).unwrap_or(&0));
    }

    Ok(result)
}