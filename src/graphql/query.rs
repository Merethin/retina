use std::sync::Arc;
use async_graphql::*;
use tokio::sync::RwLock;

use crate::{data::DataStorage, graphql::{nation::Nation, region::Region, world::World}};

pub struct Query;

#[Object]
impl Query {
    async fn world(&self) -> World {
        World {}
    }

    async fn nation<'ctx>(&self, ctx: &Context<'ctx>, name: String) -> Result<Option<Nation>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;
        let Some(nation) = r.interner.get(name).and_then(|key| r.nations.get(&key)) else { return Ok(None); };
        return Ok(Some(Nation::from_nation_data(nation)));
    }

    async fn region<'ctx>(&self, ctx: &Context<'ctx>, name: String) -> Result<Option<Region>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;
        let Some((key, region)) = r.interner.get(name).and_then(
            |key| r.regions.get(&key).map(|r| (key, r))
        ) else { return Ok(None); };

        return Ok(Some(Region::from_region_data(key, region)));
    }

    async fn nations<'ctx>(&self, ctx: &Context<'ctx>, names: Vec<String>) -> Result<Vec<Nation>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(names.into_iter().filter_map(|name| {
            let Some(nation) = r.interner.get(name).and_then(|key| r.nations.get(&key)) else { return None; };
            Some(Nation::from_nation_data(nation))
        }).collect())
    }

    async fn regions<'ctx>(&self, ctx: &Context<'ctx>, names: Vec<String>) -> Result<Vec<Region>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(names.into_iter().filter_map(|name| {
            let Some((key, region)) = r.interner.get(name).and_then(
                |key| r.regions.get(&key).map(|r| (key, r))
            ) else { return None; };

            Some(Region::from_region_data(key, region))
        }).collect())
    }
}