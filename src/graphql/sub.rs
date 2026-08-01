use async_graphql::{Context, Subscription};
use futures_util::Stream;
use tokio::sync::broadcast;

use crate::{events::SubscriptionEvent, graphql::{Nation, Region}};

pub struct Subscription;

#[Subscription]
impl Subscription {
    async fn delegate_change<'ctx>(&self, ctx: &Context<'ctx>, region: Option<String>) -> impl Stream<Item = Region> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::DelegateChange(event) = event {
                    if let Some(region) = &region && region != &event.name { continue; }
                    yield event.region;
                }
            }
        }
    }

    async fn region_change<'ctx>(&self, ctx: &Context<'ctx>, region: Option<String>) -> impl Stream<Item = Region> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::RegionChange(event) = event {
                    if let Some(region) = &region && region != &event.name { continue; }
                    yield event.region;
                }
            }
        }
    }

    async fn nation_change<'ctx>(&self, ctx: &Context<'ctx>, nation: Option<String>) -> impl Stream<Item = Nation> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::NationChange(event) = event {
                    if let Some(nation) = &nation && nation != &event.name { continue; }
                    yield event.nation;
                }
            }
        }
    }

    async fn endo_change<'ctx>(&self, ctx: &Context<'ctx>, nation: Option<String>) -> impl Stream<Item = Nation> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::EndoChange(event) = event {
                    if let Some(nation) = &nation && nation != &event.name { continue; }
                    yield event.nation;
                }
            }
        }
    }

    async fn nation_create<'ctx>(&self, ctx: &Context<'ctx>) -> impl Stream<Item = Nation> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::NationCreate(event) = event {
                    yield event.nation;
                }
            }
        }
    }

    async fn nation_delete<'ctx>(&self, ctx: &Context<'ctx>) -> impl Stream<Item = Nation> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::NationDelete(event) = event {
                    yield event.nation;
                }
            }
        }
    }

    async fn nation_move<'ctx>(&self, ctx: &Context<'ctx>, nation: Option<String>) -> impl Stream<Item = Nation> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::NationMove(event) = event {
                    if let Some(nation) = &nation && nation != &event.name { continue; }
                    yield event.nation;
                }
            }
        }
    }

    async fn region_update<'ctx>(&self, ctx: &Context<'ctx>, region: Option<String>) -> impl Stream<Item = Region> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::RegionUpdate(event) = event {
                    if let Some(region) = &region && region != &event.name { continue; }
                    yield event.region;
                }
            }
        }
    }

    async fn wa_change<'ctx>(&self, ctx: &Context<'ctx>, nation: Option<String>) -> impl Stream<Item = Nation> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::WAChange(event) = event {
                    if let Some(nation) = &nation && nation != &event.name { continue; }
                    yield event.nation;
                }
            }
        }
    }

    async fn bootstrap<'ctx>(&self, ctx: &Context<'ctx>) -> impl Stream<Item = i64> {
        let mut rx = ctx.data::<broadcast::Sender<SubscriptionEvent>>().unwrap().subscribe();

        async_stream::stream! {
            while let Ok(event) = rx.recv().await {
                if let SubscriptionEvent::Bootstrap(event) = event {
                    yield event.last_id;
                }
            }
        }
    }
}