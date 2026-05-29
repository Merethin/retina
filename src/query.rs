use std::sync::Arc;

use serde::Serialize;
use tokio::sync::RwLock;

use crate::data::DataStorage;

pub async fn query_members(
    data: Arc<RwLock<DataStorage>>,
    region: Option<&str>
) -> anyhow::Result<Vec<String>> {
    let r = data.read().await;

    let mut members = r.wa_nations.clone();
    members.sort_unstable();
    members.dedup();

    Ok(match region {
        None => {
            members.into_iter().filter_map(|v| {
                r.interner.resolve(v).map(|s| s.to_string())
            }).collect()
        },
        Some(region) => {
            let Some(mut region) = r.interner.get(region).and_then(|v| r.regions.get(&v).cloned()) else {
                return Err(anyhow::Error::msg("No such region"));
            };

            region.sort_unstable();
            region.dedup();

            region.retain(|nation| {
                members.binary_search(nation).is_ok()
            });

            region.into_iter().filter_map(|v| {
                r.interner.resolve(v).map(|s| s.to_string())
            }).collect()
        }
    })
}

#[derive(Serialize, Clone)]
pub struct Region {
    pub region: String,
    delegate: Option<String>,
    nations: Vec<RegionMember>,
}

#[derive(Serialize, Clone)]
pub struct RegionMember {
    name: String,
    endorsements: Vec<String>,
}

pub async fn query_region(
    data: Arc<RwLock<DataStorage>>,
    name: &str,
) -> anyhow::Result<Region> {
    let r = data.read().await;

    let Some(region) = r.interner.get(name) else {
        return Err(anyhow::Error::msg("No such region"));
    };

    let delegate: Option<String> = r.delegates.get(&region).and_then(|&v| {
        r.interner.resolve(v).map(|s| s.to_string())
    });

    let mut members = r.wa_nations.clone();
    members.sort_unstable();
    members.dedup();

    let Some(mut region) = r.regions.get(&region).cloned() else {
        return Err(anyhow::Error::msg("No such region"));
    };

    region.sort_unstable();
    region.dedup();

    region.retain(|nation| {
        members.binary_search(nation).is_ok()
    });

    let mut nations = vec![];
    for nation in region {
        let Some(name) = r.interner.resolve(nation) else { continue; };
        let endorsements = r.endorsements.get(&nation).map(|list| list.into_iter().filter_map(|v| {
            r.interner.resolve(*v).map(|s| s.to_string())
        }).collect()).unwrap_or_default();

        nations.push(RegionMember {
            name: name.to_string(), endorsements
        });
    }

    Ok(Region { region: name.to_string(), delegate, nations })
}

pub async fn query_regionmates(
    data: Arc<RwLock<DataStorage>>,
    nation: &str,
) -> anyhow::Result<Region> {
    let r = data.read().await;

    let Some(nation) = r.interner.get(nation).and_then(|v| r.nations.get(&v)) else {
        return Err(anyhow::Error::msg("No such nation"));
    };

    if !nation.is_wa {
        return Err(anyhow::Error::msg("Not a World Assembly nation"));
    }

    let Some(region) = r.interner.resolve(nation.region).map(|s| s.to_string()) else {
        return Err(anyhow::Error::msg("Unable to resolve region name"));
    };

    drop(r);

    query_region(data, &region).await
}

#[derive(Serialize)]
pub struct Nation {
    region: String,
    is_delegate: bool,
    endorsements: Vec<String>,
}

pub async fn query_nation(
    data: Arc<RwLock<DataStorage>>,
    name: &str,
) -> anyhow::Result<Nation> {
    let r = data.read().await;
    let Some(symbol) = r.interner.get(name) else {
        return Err(anyhow::Error::msg("No such nation"));
    };

    let Some(nation) = r.nations.get(&symbol) else {
        return Err(anyhow::Error::msg("No such nation"));
    };

    if !nation.is_wa {
        return Err(anyhow::Error::msg("Not a World Assembly nation"));
    }

    let Some(region) = r.interner.resolve(nation.region).map(|s| s.to_string()) else {
        return Err(anyhow::Error::msg("Unable to resolve region name"));
    };

    let is_delegate = Some(nation.region) == nation.delegate;

    let endorsements = r.endorsements.get(&symbol).map(|list| list.into_iter().filter_map(|v| {
        r.interner.resolve(*v).map(|s| s.to_string())
    }).collect()).unwrap_or_default();

    Ok(Nation {
        region: region,
        is_delegate,
        endorsements
    })
}