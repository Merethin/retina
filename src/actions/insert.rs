use crate::{bootstrap::Nation, data::{Interner, NationData, Snapshot}};

pub async fn insert_nation_if_missing(
    interner: &mut Interner,
    snapshot: &mut Snapshot,
    nation: &Nation
) -> anyhow::Result<bool> {
    let name = interner.get_or_intern(&nation.name);

    if snapshot.nations.contains_key(&name) {
        return Ok(false);
    }

    let region = interner.get_or_intern(&nation.region);

    let endorsements = nation.endorsements.iter().map(|v| interner.get_or_intern(v)).collect();

    snapshot.nations.insert(name, NationData {
        name,
        region,
        is_wa: nation.is_wa,
        delegate: if nation.is_delegate { Some(region) } else { None },
        lastupdate: 0,
        endorsements
    });

    snapshot.regions.entry(region).or_default().nations.insert(name);

    if nation.is_wa {
        snapshot.wa_nations.insert(name);

        if nation.is_delegate {
            snapshot.regions.entry(region).or_default().delegate = Some(name);
        }
    }

    Ok(true)
}