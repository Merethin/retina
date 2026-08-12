use std::sync::Arc;
use async_graphql::*;

use crate::{data::{GlobalData, Snapshot}, graphql::{nation::Nation, region::Region}};

pub struct World {
    pub snapshot: Arc<Snapshot>
}

#[Object]
impl World {
    async fn generation(&self) -> i64 {
        self.snapshot.generation
    }

    async fn last_event_id(&self) -> i64 {
        self.snapshot.event
    }

    async fn nations(&self) -> Result<Vec<Nation>> {
        Ok(self.snapshot.nations.values().map(|n| {
            Nation::from_nation_data(n, self.snapshot.clone())
        }).collect::<Vec<_>>())
    }

    async fn nation_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.snapshot.nations.keys().filter_map(|v| {
            interner.resolve(*v).map(|s| s.to_string())
        }).collect::<Vec<_>>())
    }

    async fn nation_count(&self) -> Result<usize> {
        Ok(self.snapshot.nations.len())
    }

    async fn members(&self) -> Result<Vec<Nation>> {
        Ok(self.snapshot.wa_nations.iter().filter_map(|v| {
            self.snapshot.nations.get(v).map(|n| Nation::from_nation_data(n, self.snapshot.clone()))
        }).collect::<Vec<_>>())
    }

    async fn member_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.snapshot.wa_nations.iter().filter_map(|v| {
            interner.resolve(*v).map(|s| s.to_string())
        }).collect::<Vec<_>>())
    }

    async fn member_count(&self) -> Result<usize> {
        Ok(self.snapshot.wa_nations.len())
    }

    async fn regions(&self) -> Result<Vec<Region>> {
        Ok(self.snapshot.regions.iter().map(|(name, data)| {
            Region::from_region_data(*name, data, self.snapshot.clone())
        }).collect::<Vec<_>>())
    }

    async fn region_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.snapshot.regions.keys().filter_map(|v| {
            interner.resolve(*v).map(|s| s.to_string())
        }).collect::<Vec<_>>())
    }

    async fn region_count(&self) -> Result<usize> {
        Ok(self.snapshot.regions.len())
    }

    async fn delegate_regions(&self) -> Result<Vec<Region>> {
        Ok(self.snapshot.regions.iter().filter_map(|(name, data)| {
            if data.delegate.is_some() {
                Some(Region::from_region_data(*name, data, self.snapshot.clone()))
            } else {
                None
            }
        }).collect::<Vec<_>>())
    }

    async fn delegate_region_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.snapshot.regions.iter().filter_map(|(name, data)| {
            if data.delegate.is_some() {
                interner.resolve(*name).map(|s| s.to_string())
            } else {
                None
            }
        }).collect::<Vec<_>>())
    }

    async fn delegate_region_count(&self) -> Result<usize> {
        Ok(self.snapshot.regions.values().filter(|data| data.delegate.is_some()).count())
    }
}