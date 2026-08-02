use std::{collections::HashMap, sync::Arc};
use caramel::types::akari::Event;
use string_interner::symbol::SymbolU32;
use tokio::sync::RwLock;
use crate::data::DataStorage;

pub async fn invalidate_endorsements(
    event: &Event,
    data: Arc<RwLock<DataStorage>>,
    existing: &HashMap<SymbolU32, i64>
) {
    let mut w = data.write().await;
    let region = w.interner.get_or_intern(event.origin.as_ref().unwrap());

    let Some(residents) = w.regions.get(&region).map(|s| s.nations.iter().copied().collect::<Vec<_>>()) else {
        return;
    };

    let mut wa_members = Vec::new();
    for index in residents {
        if let Some(nation) = w.nations.get(&index) && nation.is_wa {
            wa_members.push(nation.name);
        }
    }

    wa_members.sort_unstable();

    for index in &wa_members {
        w.nations.get_mut(index).map(|value| {
            value.endorsements.retain(|endorser| {
                if existing.get(endorser).map(|id| *id < event.event).unwrap_or(false) {
                    wa_members.binary_search(endorser).is_ok()
                } else {
                    true
                }
            });
        });
    }
}