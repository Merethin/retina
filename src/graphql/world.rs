use std::sync::Arc;
use async_graphql::*;
use tokio::sync::RwLock;

use crate::{data::DataStorage, graphql::{nation::Nation, region::Region}};

pub struct World;

#[Object]
impl World {
    async fn nations<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<Nation>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.nations.values().map(|n| {
            Nation::from_nation_data(n)
        }).collect::<Vec<_>>())
    }

    async fn nation_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.nations.keys().filter_map(|v| {
            r.interner.resolve(*v).map(|s| s.to_string())
        }).collect::<Vec<_>>())
    }

    async fn nation_count<'ctx>(&self, ctx: &Context<'ctx>) -> Result<usize> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.nations.len())
    }

    async fn members<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<Nation>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.wa_nations.iter().filter_map(|v| {
            r.nations.get(v).map(|n| Nation::from_nation_data(n))
        }).collect::<Vec<_>>())
    }

    async fn member_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.wa_nations.iter().filter_map(|v| {
            r.interner.resolve(*v).map(|s| s.to_string())
        }).collect::<Vec<_>>())
    }

    async fn member_count<'ctx>(&self, ctx: &Context<'ctx>) -> Result<usize> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.wa_nations.len())
    }

    async fn regions<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<Region>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.regions.iter().map(|(name, data)| {
            Region::from_region_data(*name, data)
        }).collect::<Vec<_>>())
    }

    async fn region_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.regions.keys().filter_map(|v| {
            r.interner.resolve(*v).map(|s| s.to_string())
        }).collect::<Vec<_>>())
    }

    async fn region_count<'ctx>(&self, ctx: &Context<'ctx>) -> Result<usize> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.regions.len())
    }

    async fn delegate_regions<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<Region>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.regions.iter().filter_map(|(name, data)| {
            if data.delegate.is_some() {
                Some(Region::from_region_data(*name, data))
            } else {
                None
            }
        }).collect::<Vec<_>>())
    }

    async fn delegate_region_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.regions.iter().filter_map(|(name, data)| {
            if data.delegate.is_some() {
                r.interner.resolve(*name).map(|s| s.to_string())
            } else {
                None
            }
        }).collect::<Vec<_>>())
    }

    async fn delegate_region_count<'ctx>(&self, ctx: &Context<'ctx>) -> Result<usize> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(r.regions.values().filter(|data| data.delegate.is_some()).count())
    }
}