use std::collections::{HashMap, HashSet};
use ordermap::OrderSet;
use string_interner::{DefaultSymbol, DefaultStringInterner};

pub struct NationData {
    pub name: DefaultSymbol,
    pub region: DefaultSymbol,
    pub is_wa: bool,
    pub delegate: Option<DefaultSymbol>,
    pub lastupdate: u64,
    pub endorsements: OrderSet<DefaultSymbol>,
}

#[derive(Default, Clone)]
pub struct RegionData {
    pub nations: HashSet<DefaultSymbol>,
    pub lastupdate: u64,
    pub delegate: Option<DefaultSymbol>
}

pub struct DataStorage {
    pub interner: DefaultStringInterner,
    pub nations: HashMap<DefaultSymbol, NationData>,
    pub regions: HashMap<DefaultSymbol, RegionData>,
    pub wa_nations: HashSet<DefaultSymbol>,
}

impl DataStorage {
    pub fn new() -> Self {
        Self { 
            interner: DefaultStringInterner::new(),
            nations: HashMap::new(),
            regions: HashMap::new(),
            wa_nations: HashSet::new(),
        }
    }
}