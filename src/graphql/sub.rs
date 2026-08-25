use std::{collections::HashSet, sync::Arc};

use async_graphql::{Context, SimpleObject, Subscription};
use futures_util::Stream;
use log::warn;
use string_interner::symbol::SymbolU32;
use tokio::sync::broadcast::{self, error::RecvError};

use crate::{data::GlobalData, events::{SubscriptionDetails, SubscriptionEvent}, graphql::modified::{ModifiedNation, ModifiedRegion, ModifiedWorld}};

pub struct Subscription;

#[derive(SimpleObject)]
pub struct SiteEvent {
    id: i64,
    time: u64,
    actor: Option<ModifiedNation>,
    receptor: Option<ModifiedNation>,
    origin: Option<ModifiedRegion>,
    destination: Option<ModifiedRegion>,
    category: String,
    world: ModifiedWorld,
    data: Vec<String>,
}

#[Subscription]
impl Subscription {
    async fn region_change<'ctx>(&self, ctx: &Context<'ctx>, regions: Vec<String>) -> impl Stream<Item = ModifiedRegion> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionDetails>>().unwrap().subscribe();

        let filter: HashSet<SymbolU32> = {
            let interner = ctx.data::<Arc<GlobalData>>().unwrap().interner.read().await;
            regions.iter().filter_map(|r| {
                interner.get(r)
            }).collect()
        };

        async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(item) => if let SubscriptionEvent::RegionChange(id) = item.event {
                        if !filter.is_empty() && !filter.contains(&id) { continue; }
                        yield ModifiedRegion {
                            id: id,
                            before: item.before.clone(),
                            after: item.after.clone(),
                        };
                    },
                    Err(err) => {
                        match err {
                            RecvError::Closed => warn!("region_change subscription channel closed"),
                            RecvError::Lagged(n) => warn!("region_change subscription channel skipped {n} items"),
                        }

                        break;
                    }
                }
            }
        }
    }

    async fn nation_change<'ctx>(&self, ctx: &Context<'ctx>, nations: Vec<String>) -> impl Stream<Item = ModifiedNation> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionDetails>>().unwrap().subscribe();

        let filter: HashSet<SymbolU32> = {
            let interner = ctx.data::<Arc<GlobalData>>().unwrap().interner.read().await;
            nations.iter().filter_map(|r| {
                interner.get(r)
            }).collect()
        };

        async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(item) => if let SubscriptionEvent::NationChange(id) = item.event {
                        if !filter.is_empty() && !filter.contains(&id) { continue; }
                        yield ModifiedNation {
                            id: id,
                            before: item.before.clone(),
                            after: item.after.clone(),
                        }
                    },
                    Err(err) => {
                        match err {
                            RecvError::Closed => warn!("nation_change subscription channel closed"),
                            RecvError::Lagged(n) => warn!("nation_change subscription channel skipped {n} items"),
                        }

                        break;
                    }
                }
            }
        }
    }

    async fn site_event<'ctx>(&self, ctx: &Context<'ctx>, categories: Vec<String>) -> impl Stream<Item = SiteEvent> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionDetails>>().unwrap().subscribe();

        async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(item) => if let SubscriptionEvent::SiteEvent(event) = item.event {
                        if !categories.is_empty() && !categories.contains(&event.category) { continue; }

                        let interner = ctx.data::<Arc<GlobalData>>().unwrap().interner.read().await;

                        yield SiteEvent {
                            id: event.event,
                            time: event.time,
                            actor: event.actor.and_then(|n| interner.get(n).map(|id| {
                                ModifiedNation {
                                    id: id,
                                    before: item.before.clone(),
                                    after: item.after.clone(),
                                }
                            })),
                            receptor: event.receptor.and_then(|n| interner.get(n).map(|id| {
                                ModifiedNation {
                                    id: id,
                                    before: item.before.clone(),
                                    after: item.after.clone(),
                                }
                            })),
                            origin: event.origin.and_then(|r| interner.get(r).map(|id| {
                                ModifiedRegion {
                                    id: id,
                                    before: item.before.clone(),
                                    after: item.after.clone(),
                                }
                            })),
                            destination: event.destination.and_then(|r| interner.get(r).map(|id| {
                                ModifiedRegion {
                                    id: id,
                                    before: item.before.clone(),
                                    after: item.after.clone(),
                                }
                            })),
                            category: event.category,
                            world: ModifiedWorld {
                                before: item.before,
                                after: item.after,
                            },
                            data: event.data
                        };
                    },
                    Err(err) => {
                        match err {
                            RecvError::Closed => warn!("site_event subscription channel closed"),
                            RecvError::Lagged(n) => warn!("site_event subscription channel skipped {n} items"),
                        }

                        break;
                    }
                }
            }
        }
    }

    async fn bootstrap<'ctx>(&self, ctx: &Context<'ctx>) -> impl Stream<Item = ModifiedWorld> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionDetails>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(item) = rx.recv().await {
                if let SubscriptionEvent::Bootstrap = item.event {
                    yield ModifiedWorld {
                        before: item.before,
                        after: item.after,
                    };
                }
            }
        }
    }
}