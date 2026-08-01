use std::sync::Arc;
use tokio::sync::RwLock;
use crate::{bootstrap::Nation, data::{DataStorage, NationData}};

pub async fn insert_nation_if_missing(
    data: Arc<RwLock<DataStorage>>,
    nation: &Nation
) -> anyhow::Result<bool> {
    let mut w = data.write().await;

    let name = w.interner.get_or_intern(&nation.name);

    if w.nations.contains_key(&name) {
        return Ok(false);
    }

    let region = w.interner.get_or_intern(&nation.region);

    let endorsements = nation.endorsements.iter().map(|v| w.interner.get_or_intern(v)).collect();

    w.nations.insert(name, NationData {
        name,
        region,
        is_wa: nation.is_wa,
        delegate: if nation.is_delegate { Some(region) } else { None },
        lastupdate: 0,
        endorsements
    });

    w.regions.entry(region).or_default().nations.insert(name);

    if nation.is_wa {
        w.wa_nations.insert(name);

        if nation.is_delegate {
            w.regions.entry(region).or_default().delegate = Some(name);
        }
    }

    Ok(true)
}