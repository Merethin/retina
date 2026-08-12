use std::sync::Arc;

use async_graphql::*;
use im::OrdSet;
use string_interner::symbol::SymbolU32;

use crate::{data::{GlobalData, NationData, Snapshot}, graphql::region::Region};

#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct Nation {
    #[graphql(skip)]
    i_name: SymbolU32,
    #[graphql(skip)]
    i_region: SymbolU32,
    pub is_wa: bool,
    #[graphql(skip)]
    i_delegate: Option<SymbolU32>,
    pub lastupdate: u64,
    #[graphql(skip)]
    i_endorsements: OrdSet<SymbolU32>,
    #[graphql(skip)]
    snapshot: Arc<Snapshot>,
}

impl Nation {
    pub fn from_nation_data(data: &NationData, snapshot: Arc<Snapshot>) -> Self {
        Self {
            i_name: data.name,
            i_region: data.region,
            is_wa: data.is_wa,
            i_delegate: data.delegate,
            lastupdate: data.lastupdate,
            i_endorsements: data.endorsements.clone(),
            snapshot: snapshot
        }
    }
}

#[derive(SimpleObject)]
pub struct InactiveNation {
    name: String,
}

#[derive(Interface)]
#[graphql(field(name = "name", ty = "String"))]
enum ShadowEndorser {
    Existing(Nation),
    CTE(InactiveNation),
}

#[ComplexObject]
impl Nation {
    async fn name<'ctx>(&self, ctx: &Context<'ctx>) -> Result<String> {
        ctx.data::<Arc<GlobalData>>()?.interner.read().await.resolve(self.i_name).map(|v| v.to_string()).ok_or("No such nation".into())
    }

    async fn region_name<'ctx>(&self, ctx: &Context<'ctx>) -> Result<String> {
        ctx.data::<Arc<GlobalData>>()?.interner.read().await.resolve(self.i_region).map(|v| v.to_string()).ok_or("No such region".into())
    }

    async fn region(&self) -> Result<Region> {
        let Some(region) = self.snapshot.regions.get(&self.i_region) else {
            return Err("No such region".into());
        };

        Ok(Region::from_region_data(self.i_region, region, self.snapshot.clone()))
    }

    async fn is_delegate(&self) -> bool {
        self.i_delegate.is_some()
    }

    async fn delegacy_name<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Option<String>> {
        match self.i_delegate {
            None => Ok(None),
            Some(region) => {
                let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;
                interner.resolve(region).map(|v| Some(v.to_string())).ok_or("No such region".into())
            }
        }
    }

    async fn delegacy(&self) -> Result<Option<Region>> {
        match self.i_delegate {
            None => Ok(None),
            Some(region) => {
                let Some(data) = self.snapshot.regions.get(&region) else {
                    return Err("No such region".into());
                };

                Ok(Some(Region::from_region_data(region, data, self.snapshot.clone())))
            }
        }
    }

    async fn full_endorsements<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.i_endorsements.iter().filter_map(|v| {
            interner.resolve(*v).map(|s| s.to_string())
        }).collect::<Vec<_>>())
    }

    async fn full_endorsement_count(&self) -> usize {
        self.i_endorsements.len()
    }

    async fn full_endorsers(&self) -> Result<Vec<Option<Nation>>> {
        Ok(self.i_endorsements.iter().map(|v| {
            self.snapshot.nations.get(v).map(|n| Self::from_nation_data(n, self.snapshot.clone()))
        }).collect::<Vec<_>>())
    }

    async fn valid_endorsements<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let Some(region) = self.snapshot.regions.get(&self.i_region) else {
            return Err("No such region".into());
        };

        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.i_endorsements.iter().filter_map(|v| {
            if !region.nations.contains(v) || !self.snapshot.wa_nations.contains(v) {
                None
            } else {
                interner.resolve(*v).map(|s| s.to_string())
            }
        }).collect::<Vec<_>>())
    }

    async fn valid_endorsement_count(&self) -> Result<usize> {
        let Some(region) = self.snapshot.regions.get(&self.i_region) else {
            return Err("No such region".into());
        };

        Ok(self.i_endorsements.iter().filter_map(|v| {
            if !region.nations.contains(v) || !self.snapshot.wa_nations.contains(v) {
                None
            } else {
                Some(v)
            }
        }).count())
    }

    async fn valid_endorsers(&self) -> Result<Vec<Nation>> {
        let Some(region) = self.snapshot.regions.get(&self.i_region) else {
            return Err("No such region".into());
        };

        Ok(self.i_endorsements.iter().filter_map(|v| {
            if !region.nations.contains(v) || !self.snapshot.wa_nations.contains(v) {
                None
            } else {
                self.snapshot.nations.get(v).map(|n| Self::from_nation_data(n, self.snapshot.clone()))
            }
        }).collect::<Vec<_>>())
    }

    async fn shadow_endorsements<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<String>> {
        let Some(region) = self.snapshot.regions.get(&self.i_region) else {
            return Err("No such region".into());
        };

        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.i_endorsements.iter().filter_map(|v| {
            if !region.nations.contains(v) || !self.snapshot.wa_nations.contains(v) {
                interner.resolve(*v).map(|s| s.to_string())
            } else {
                None
            }
        }).collect::<Vec<_>>())
    }

    async fn shadow_endorsement_count(&self) -> Result<usize> {
        let Some(region) = self.snapshot.regions.get(&self.i_region) else {
            return Err("No such region".into());
        };

        Ok(self.i_endorsements.iter().filter_map(|v| {
            if !region.nations.contains(v) || !self.snapshot.wa_nations.contains(v) {
                Some(v)
            } else {
                None
            }
        }).count())
    }

    async fn shadow_endorsers<'ctx>(&self, ctx: &Context<'ctx>) -> Result<Vec<ShadowEndorser>> {
        let Some(region) = self.snapshot.regions.get(&self.i_region) else {
            return Err("No such region".into());
        };

        let interner = ctx.data::<Arc<GlobalData>>()?.interner.read().await;

        Ok(self.i_endorsements.iter().filter_map(|v| {
            if !region.nations.contains(v) || !self.snapshot.wa_nations.contains(v) {
                self.snapshot.nations.get(v).map(|n| Self::from_nation_data(n, self.snapshot.clone())).map(|n| {
                    ShadowEndorser::Existing(n)
                }).or_else(|| {
                    interner.resolve(*v).map(|s| s.to_string()).map(|s| {
                        ShadowEndorser::CTE(InactiveNation { name: s })
                    })
                })
            } else {
                None
            }
        }).collect::<Vec<_>>())
    }
}