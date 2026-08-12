use std::sync::Arc;
use async_graphql::*;

use crate::{data::GlobalData, graphql::{nation::Nation, region::Region, world::World}};

pub struct Query;

#[Object]
impl Query {
    async fn world<'ctx>(&self, ctx: &Context<'ctx>) -> Result<World> {
        let snapshot = ctx.data::<Arc<GlobalData>>()?.last_snapshot.read().await.clone();

        Ok(World { snapshot })
    }

    async fn nation<'ctx>(&self, ctx: &Context<'ctx>, name: String) -> Result<Option<Nation>> {
        let snapshot = ctx.data::<Arc<GlobalData>>()?.last_snapshot.read().await.clone();
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        let Some(nation) = interner.get(name).and_then(|key| snapshot.nations.get(&key)) else { return Ok(None); };
        return Ok(Some(Nation::from_nation_data(nation, snapshot.clone())));
    }

    async fn region<'ctx>(&self, ctx: &Context<'ctx>, name: String) -> Result<Option<Region>> {
        let snapshot = ctx.data::<Arc<GlobalData>>()?.last_snapshot.read().await.clone();
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        let Some((key, region)) = interner.get(name).and_then(
            |key| snapshot.regions.get(&key).map(|r| (key, r))
        ) else { return Ok(None); };

        return Ok(Some(Region::from_region_data(key, region, snapshot.clone())));
    }

    async fn nations<'ctx>(&self, ctx: &Context<'ctx>, names: Vec<String>) -> Result<Vec<Nation>> {
        let snapshot = ctx.data::<Arc<GlobalData>>()?.last_snapshot.read().await.clone();
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(names.into_iter().filter_map(|name| {
            let Some(nation) = interner.get(name).and_then(|key| snapshot.nations.get(&key)) else { return None; };
            Some(Nation::from_nation_data(nation, snapshot.clone()))
        }).collect())
    }

    async fn regions<'ctx>(&self, ctx: &Context<'ctx>, names: Vec<String>) -> Result<Vec<Region>> {
        let snapshot = ctx.data::<Arc<GlobalData>>()?.last_snapshot.read().await.clone();
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(names.into_iter().filter_map(|name| {
            let Some((key, region)) = interner.get(name).and_then(
                |key| snapshot.regions.get(&key).map(|r| (key, r))
            ) else { return None; };

            Some(Region::from_region_data(key, region, snapshot.clone()))
        }).collect())
    }
}