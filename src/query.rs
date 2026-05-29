use std::{collections::HashMap, error::Error};

use serde::Serialize;
use sqlx::{PgPool, Postgres};

pub async fn query_members(
    pool: &PgPool,
    region: Option<&str>
) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    Ok(match region {
        None => {
            sqlx::query_scalar(
                "SELECT name FROM retina_nations"
            ).fetch_all(pool).await?
        },
        Some(region) => {
            sqlx::query_scalar(
                "SELECT name FROM retina_nations WHERE region = $1"
            ).bind(region).fetch_all(pool).await?
        }
    })
}

#[derive(Serialize)]
pub struct Delegate {
    name: String,
    region: String,
    endos_received: i64,
    endos_given: i64,
}

pub async fn query_delegates(
    pool: &PgPool
) -> Result<Vec<Delegate>, Box<dyn Error + Send + Sync>> {
    let delegates: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, delegacy FROM retina_nations WHERE delegacy IS NOT NULL"
    ).fetch_all(pool).await?;

    let names = delegates.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();

    let endos_received: HashMap<String, i64> = sqlx::query_as::<Postgres, (String, i64)>(
        "SELECT target, COUNT(*) AS count FROM retina_endorsements WHERE target = ANY($1) GROUP BY target"
    ).bind(&names).fetch_all(pool).await?.into_iter().collect();

    let endos_given: HashMap<String, i64> = sqlx::query_as::<Postgres, (String, i64)>(
        "SELECT endorser, COUNT(*) AS count FROM retina_endorsements WHERE endorser = ANY($1) GROUP BY endorser"
    ).bind(&names).fetch_all(pool).await?.into_iter().collect();

    let mut result: Vec<Delegate> = Vec::new();
    for (name, region) in delegates {
        let er = *endos_received.get(&name).unwrap_or(&0);
        let eg = *endos_given.get(&name).unwrap_or(&0);

        result.push(Delegate {
            name, region,
            endos_received: er,
            endos_given: eg
        });
    }

    Ok(result)
}

#[derive(Serialize)]
pub struct Region {
    region: String,
    delegate: Option<String>,
    nations: Vec<RegionMember>,
}

#[derive(Serialize)]
pub struct RegionMember {
    name: String,
    endorsements: Vec<String>,
}

pub async fn query_region(
    pool: &PgPool,
    region: &str,
) -> Result<Region, Box<dyn Error + Send + Sync>> {
    let delegate: Option<String> = sqlx::query_scalar(
        "SELECT name FROM retina_nations WHERE delegacy = $1"
    ).bind(region).fetch_optional(pool).await?;

    let members: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM retina_nations WHERE region = $1"
    ).bind(region).fetch_all(pool).await?;

    let endos_received: Vec<(String, String)> = sqlx::query_as(
        "SELECT target, endorser FROM retina_endorsements WHERE target = ANY($1)"
    ).bind(&members).fetch_all(pool).await?;

    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for (target, endorser) in endos_received {
        grouped.entry(target).or_default().push(endorser);
    }

    let mut nations = vec![];
    for name in members {
        let endorsements = grouped.get(&name).cloned().unwrap_or_default();

        nations.push(RegionMember {
            name, endorsements
        });
    }

    Ok(Region { region: region.to_string(), delegate, nations })
}

pub async fn query_regionmates(
    pool: &PgPool,
    nation: &str,
) -> Result<Region, Box<dyn Error + Send + Sync>> {
    let region: String = sqlx::query_scalar(
        "SELECT region FROM retina_nations WHERE name = $1"
    ).bind(nation).fetch_one(pool).await?;

    query_region(pool, &region).await
}

#[derive(Serialize)]
pub struct Nation {
    region: String,
    is_delegate: bool,
    endos_received: Vec<String>,
    endos_given: Vec<String>,
}

pub async fn query_nation(
    pool: &PgPool,
    nation: &str,
) -> Result<Nation, Box<dyn Error + Send + Sync>> {
    let data: (String, Option<String>) = sqlx::query_as(
        "SELECT region, delegacy FROM retina_nations WHERE name = $1"
    ).bind(nation).fetch_one(pool).await?;

    let endorsements: Vec<(String, String)> = sqlx::query_as(
        "SELECT target, endorser FROM retina_endorsements WHERE target = $1 OR endorser = $1"
    ).bind(nation).fetch_all(pool).await?;

    let mut endos_received = vec![];
    let mut endos_given = vec![];

    for (target, endorser) in endorsements {
        if &target == nation {
            endos_received.push(endorser);
        } else {
            endos_given.push(target);
        }
    }

    let is_delegate = Some(&data.0) == data.1.as_ref();

    Ok(Nation {
        region: data.0,
        is_delegate,
        endos_received,
        endos_given
    })
}