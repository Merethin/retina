use std::{collections::{HashMap, HashSet}, error::Error};
use sqlx::{PgPool, Row};
use serde::{Serialize, Deserialize};

use caramel::types::akari::Event;

use crate::{data::{DataStorage, NationData}, events::KEYS};

#[derive(Debug, Serialize, Deserialize)]
pub struct Nation {
    pub name: String,
    pub is_wa: bool,
    pub is_delegate: bool,
    pub region: String,
    pub endorsements: Vec<String>,
}

pub async fn fetch_data_dump_and_events(
    pool: &PgPool
) -> Result<(Vec<Nation>, Vec<Event>, Vec<Event>, i64), Box<dyn Error + Send + Sync>> {
    let (update_start, update_end) = query_last_major_from_data_dump(pool).await?;
    let nations = query_data_dump_nation_state(pool).await?;
    let update_events = query_update_bootstrap_events(pool, update_start, update_end, KEYS.to_vec()).await?;
    let subseq_events = query_subsequent_bootstrap_events(pool, update_end, KEYS.to_vec()).await?;

    Ok((nations, update_events, subseq_events, update_start))
}

pub fn bootstrap_storage_from_initial_data(
    nations: Vec<Nation>,
    update_times: HashMap<String, i64>,
) -> Result<DataStorage, Box<dyn Error + Send + Sync>> {
    let mut data = DataStorage::new();

    data.nations.reserve(nations.len());

    for (region, time) in &update_times {
        data.regions.entry(data.interner.get_or_intern(region)).or_default().lastupdate = *time as u64;
    }

    for nation in &nations {
        let name = data.interner.get_or_intern(&nation.name);
        let region = data.interner.get_or_intern(&nation.region);

        data.nations.insert(name, NationData {
            name,
            region,
            is_wa: nation.is_wa,
            delegate: if nation.is_delegate { Some(region) } else { None },
            lastupdate: *update_times.get(&nation.region).unwrap_or(&0) as u64
        });

        data.regions.entry(region).or_default().nations.push(name);

        if nation.is_delegate {
            data.regions.entry(region).or_default().delegate = Some(name);
        }
    }

    data.wa_nations = nations.iter().filter_map(|v| {
        if v.is_wa {
            Some(data.interner.get_or_intern(&v.name))
        } else { None }
    }).collect();

    for nation in nations {
        if nation.is_wa {
            let name = data.interner.get_or_intern(&nation.name);
            let endorsements = nation.endorsements.iter().map(|v| data.interner.get_or_intern(v)).collect();
            data.endorsements.insert(name, endorsements);
        }
    }

    Ok(data)
}

// Save a list of nations that are expected to not have updated this major update from the current snapshot.
// If after bootstrap, any of these nations are missing from the new snapshot, they will be reinserted.
pub fn save_nonupdaters(
    update_start: i64,
    update_events: &Vec<Event>,
    storage: &DataStorage
) -> Vec<Nation> {
    let mut nations: HashSet<String> = HashSet::new();

    for event in update_events {
        match event.category.as_str() {
            "move" | "nfound" | "nrefound" => nations.insert(event.actor.clone().unwrap()),
            _ => false,
        };
    }

    let mut result = Vec::new();

    for name in nations {
        let Some((nation, symbol)) = storage.interner.get(&name).and_then(
            |s| storage.nations.get(&s).map(|v| (v, s))
        ) else { continue; };

        if nation.lastupdate > (update_start as u64) {
            continue;
        }

        let Some(region) = storage.interner.resolve(nation.region).map(|s| s.to_string()) else {
            continue;
        };

        let is_delegate = Some(nation.region) == nation.delegate;

        let endorsements = storage.endorsements.get(&symbol).map(|list| list.into_iter().filter_map(|v| {
            storage.interner.resolve(*v).map(|s| s.to_string())
        }).collect()).unwrap_or_default();

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
        "SELECT canon_name, is_wa, is_delegate, region, endorsements FROM nations_dump"
    ).fetch_all(pool).await?;

    Ok(result.into_iter().map(|row| {
        Nation {
            name: row.get(0),
            is_wa: row.get(1),
            is_delegate: row.get(2),
            region: row.get(3),
            endorsements: row.get(4),
        }
    }).collect())
}

pub async fn query_update_times(
    pool: &PgPool,
) -> Result<HashMap<String, i64>, Box<dyn Error + Send + Sync>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT canon_name, lastmajorupdate FROM regions_dump"
    ).fetch_all(pool).await?;

    Ok(rows.into_iter().collect())
}