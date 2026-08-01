use std::{collections::HashSet, sync::Arc};

use async_graphql::*;
use string_interner::symbol::SymbolU32;
use tokio::sync::RwLock;

use crate::{data::{DataStorage, RegionData}, graphql::nation::Nation};

#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct Region {
    #[graphql(skip)]
    i_name: SymbolU32,
    #[graphql(skip)]
    i_nations: HashSet<SymbolU32>,
    pub lastupdate: u64,
    #[graphql(skip)]
    i_delegate: Option<SymbolU32>
}

impl Region {
    pub fn from_region_data(name: SymbolU32, data: &RegionData) -> Self {
        Self {
            i_name: name,
            i_nations: data.nations.clone(),
            lastupdate: data.lastupdate,
            i_delegate: data.delegate
        }
    }
}

#[ComplexObject]
impl Region {
    async fn name<'ctx>(&self, ctx: &Context<'ctx>) -> Result<String> {
        ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await.interner.resolve(self.i_name).map(|v| v.to_string()).ok_or("No such region".into())
    }

    async fn has_delegate(&self) -> bool {
        self.i_delegate.is_some()
    }

    async fn delegate_name<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Option<String>> {
        match self.i_delegate {
            None => Ok(None),
            Some(name) => {
                let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;
                r.interner.resolve(name).map(|v| Some(v.to_string())).ok_or("No such nation".into())
            }
        }
    }

    async fn delegate<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Option<Nation>> {
        match self.i_delegate {
            None => Ok(None),
            Some(name) => {
                let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;
                let Some(n) = r.nations.get(&name) else {
                    return Err("No such nation".into());
                };

                Ok(Some(Nation::from_nation_data(n)))
            }
        }
    }

    async fn residents<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<Nation>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(self.i_nations.iter().filter_map(|v| {
            r.nations.get(v).map(|n| Nation::from_nation_data(n))
        }).collect::<Vec<_>>())
    }

    async fn resident_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(self.i_nations.iter().filter_map(|v| {
            r.interner.resolve(*v).map(|s| s.to_string())
        }).collect::<Vec<_>>())
    }

    async fn resident_count(&self) -> usize {
        self.i_nations.len()
    }

    async fn members<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<Nation>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(self.i_nations.iter().filter_map(|v| {
            if !r.wa_nations.contains(v) {
                None
            } else {
                r.nations.get(v).map(|n| Nation::from_nation_data(n))
            }
        }).collect::<Vec<_>>())
    }

    async fn member_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(self.i_nations.iter().filter_map(|v| {
            if !r.wa_nations.contains(v) {
                None
            } else {
                r.interner.resolve(*v).map(|s| s.to_string())
            }
        }).collect::<Vec<_>>())
    }

    async fn member_count<'ctx>(&self, ctx: &Context<'ctx>) -> Result<usize> {
        let r = ctx.data::<Arc<RwLock<DataStorage>>>()?.read().await;

        Ok(self.i_nations.iter().filter(|v| {
            r.wa_nations.contains(v)
        }).count())
    }
}