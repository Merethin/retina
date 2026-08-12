use std::sync::{Arc, atomic::AtomicI64};

use string_interner::{StringInterner, backend::StringBackend, symbol::SymbolU32};
use im::{HashMap, HashSet, OrdSet};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct NationData {
    pub name: SymbolU32,
    pub region: SymbolU32,
    pub is_wa: bool,
    pub delegate: Option<SymbolU32>,
    pub lastupdate: u64,
    pub endorsements: OrdSet<SymbolU32>,
}

#[derive(Default, Clone)]
pub struct RegionData {
    pub nations: HashSet<SymbolU32>,
    pub lastupdate: u64,
    pub delegate: Option<SymbolU32>
}

#[derive(Clone)]
pub struct Snapshot {
    pub generation: i64,
    pub event: i64,
    pub nations: HashMap<SymbolU32, NationData>,
    pub regions: HashMap<SymbolU32, RegionData>,
    pub wa_nations: HashSet<SymbolU32>,
}

impl Snapshot {
    pub fn start_generation(generation: i64) -> Self {
        Self {
            generation,
            event: 0,
            nations: HashMap::new(),
            regions: HashMap::new(),
            wa_nations: HashSet::new()
        }
    }

    pub fn modify(&self, event: i64) -> Self {
        let mut copy = self.clone();
        copy.event = event;
        copy
    }
}

pub type Interner = StringInterner<StringBackend<SymbolU32>>;

pub struct GlobalData {
    pub interner: RwLock<Interner>,
    pub generation_counter: AtomicI64,
    pub last_snapshot: RwLock<Arc<Snapshot>>,
}

impl GlobalData {
    pub fn new() -> Self {
        Self {
            interner: RwLock::new(StringInterner::new()),
            generation_counter: AtomicI64::new(-1),
            last_snapshot: RwLock::new(Arc::new(Snapshot::start_generation(-1)))
        }
    }
}
