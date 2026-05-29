use std::collections::HashMap;
use string_interner::{DefaultSymbol, DefaultStringInterner};

pub struct NationData {
    pub name: DefaultSymbol,
    pub region: DefaultSymbol,
    pub is_wa: bool,
    pub delegate: Option<DefaultSymbol>,
    pub lastupdate: u64,
}

pub struct DataStorage {
    pub interner: DefaultStringInterner,
    pub nations: HashMap<DefaultSymbol, NationData>,

    pub regions: HashMap<DefaultSymbol, Vec<DefaultSymbol>>,
    pub delegates: HashMap<DefaultSymbol, DefaultSymbol>,
    pub wa_nations: Vec<DefaultSymbol>,

    pub endorsements: HashMap<DefaultSymbol, Vec<DefaultSymbol>>,
}

impl DataStorage {
    pub fn new() -> Self {
        Self { 
            interner: DefaultStringInterner::new(),
            nations: HashMap::new(),
            regions: HashMap::new(),
            delegates: HashMap::new(),
            wa_nations: Vec::new(),
            endorsements: HashMap::new(),
        }
    }
}