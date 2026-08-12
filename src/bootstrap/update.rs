use std::collections::HashMap;
use caramel::types::akari::Event;
use string_interner::symbol::SymbolU32;
use crate::data::{Interner, Snapshot};

pub async fn invalidate_endorsements(
    event: &Event,
    interner: &Interner,
    snapshot: &mut Snapshot,
    existing: &HashMap<SymbolU32, i64>
) {
    let Some(region) = interner.get(event.origin.as_ref().unwrap()) else {
        return;
    };

    let Some(residents) = snapshot.regions.get(&region).map(|s| s.nations.iter().copied().collect::<Vec<_>>()) else {
        return;
    };

    let mut wa_members = Vec::new();
    for index in residents {
        if let Some(nation) = snapshot.nations.get(&index) && nation.is_wa {
            wa_members.push(nation.name);
        }
    }

    wa_members.sort_unstable();

    for index in &wa_members {
        snapshot.nations.get_mut(index).map(|value| {
            value.endorsements = value.endorsements.iter().filter(|endorser| {
                if existing.get(endorser).map(|id| *id < event.event).unwrap_or(false) {
                    wa_members.binary_search(endorser).is_ok()
                } else {
                    true
                }
            }).cloned().collect();
        });
    }
}