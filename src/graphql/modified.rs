use std::sync::Arc;
use async_graphql::*;
use string_interner::symbol::SymbolU32;

use crate::{data::Snapshot, graphql::{nation::Nation, region::Region, world::World}};

pub struct ModifiedNation {
    pub id: SymbolU32,
    pub before: Arc<Snapshot>,
    pub after: Arc<Snapshot>,
}

pub struct ModifiedRegion {
    pub id: SymbolU32,
    pub before: Arc<Snapshot>,
    pub after: Arc<Snapshot>,
}

pub struct ModifiedWorld {
    pub before: Arc<Snapshot>,
    pub after: Arc<Snapshot>,
}

#[Object]
impl ModifiedNation {
    async fn before(&self) -> Option<Nation> {
        let Some(nation) = self.before.nations.get(&self.id) else {
            return None;
        };

        Some(Nation::from_nation_data(nation, self.before.clone()))
    }

    async fn after(&self) -> Option<Nation> {
        let Some(nation) = self.after.nations.get(&self.id) else {
            return None;
        };

        Some(Nation::from_nation_data(nation, self.after.clone()))
    }
}

#[Object]
impl ModifiedRegion {
    async fn before(&self) -> Option<Region> {
        let Some(region) = self.before.regions.get(&self.id) else {
            return None;
        };

        Some(Region::from_region_data(self.id, region, self.before.clone()))
    }

    async fn after(&self) -> Option<Region> {
        let Some(region) = self.after.regions.get(&self.id) else {
            return None;
        };

        Some(Region::from_region_data(self.id, region, self.after.clone()))
    }
}

#[Object]
impl ModifiedWorld {
    async fn before(&self) -> World {
        World { snapshot: self.before.clone() }
    }

    async fn after(&self) -> World {
        World { snapshot: self.after.clone() }
    }
}