use std::sync::Arc;

use async_graphql::*;
use string_interner::symbol::SymbolU32;
use im::HashSet;

use crate::{data::{GlobalData, RegionData, Snapshot}, graphql::nation::Nation};

#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct Region {
    #[graphql(skip)]
    i_name: SymbolU32,
    #[graphql(skip)]
    i_nations: HashSet<SymbolU32>,
    pub lastupdate: u64,
    #[graphql(skip)]
    i_delegate: Option<SymbolU32>,
    #[graphql(skip)]
    snapshot: Arc<Snapshot>
}

impl Region {
    pub fn from_region_data(name: SymbolU32, data: &RegionData, snapshot: Arc<Snapshot>) -> Self {
        Self {
            i_name: name,
            i_nations: data.nations.clone(),
            lastupdate: data.lastupdate,
            i_delegate: data.delegate,
            snapshot
        }
    }
}

#[ComplexObject]
impl Region {
    async fn name<'ctx>(&self, ctx: &Context<'ctx>) -> Result<String> {
        ctx.data::<Arc<GlobalData>>()?.interner.read().await.resolve(self.i_name).map(|v| v.to_string()).ok_or("No such region".into())
    }

    async fn has_delegate(&self) -> bool {
        self.i_delegate.is_some()
    }

    async fn delegate_name<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Option<String>> {
        match self.i_delegate {
            None => Ok(None),
            Some(name) => {
                let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;
                interner.resolve(name).map(|v| Some(v.to_string())).ok_or("No such nation".into())
            }
        }
    }

    async fn delegate(&self) -> Result<Option<Nation>> {
        match self.i_delegate {
            None => Ok(None),
            Some(name) => {
                let Some(n) = self.snapshot.nations.get(&name) else {
                    return Err("No such nation".into());
                };

                Ok(Some(Nation::from_nation_data(n, self.snapshot.clone())))
            }
        }
    }

    async fn residents(&self) -> Result<Vec<Nation>> {
        Ok(self.i_nations.iter().filter_map(|v| {
            self.snapshot.nations.get(v).map(|n| Nation::from_nation_data(n, self.snapshot.clone()))
        }).collect::<Vec<_>>())
    }

    async fn resident_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.i_nations.iter().filter_map(|v| {
            interner.resolve(*v).map(|s| s.to_string())
        }).collect::<Vec<_>>())
    }

    async fn resident_count(&self) -> usize {
        self.i_nations.len()
    }

    async fn members(&self) -> Result<Vec<Nation>> {
        Ok(self.i_nations.iter().filter_map(|v| {
            if !self.snapshot.wa_nations.contains(v) {
                None
            } else {
                self.snapshot.nations.get(v).map(|n| Nation::from_nation_data(n, self.snapshot.clone()))
            }
        }).collect::<Vec<_>>())
    }

    async fn member_names<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.i_nations.iter().filter_map(|v| {
            if !self.snapshot.wa_nations.contains(v) {
                None
            } else {
                interner.resolve(*v).map(|s| s.to_string())
            }
        }).collect::<Vec<_>>())
    }

    async fn member_count(&self) -> Result<usize> {
        Ok(self.i_nations.iter().filter(|v| {
            self.snapshot.wa_nations.contains(v)
        }).count())
    }
}