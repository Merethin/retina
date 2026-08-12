use std::sync::Arc;

use caramel::types::akari::Event;
use string_interner::symbol::SymbolU32;

use crate::data::Snapshot;

#[derive(Clone)]
pub enum SubscriptionEvent {
    RegionChange(SymbolU32),
    NationChange(SymbolU32),
    SiteEvent(Event),
    Bootstrap,
}

#[derive(Clone)]
pub struct SubscriptionDetails {
    pub event: SubscriptionEvent,
    pub before: Arc<Snapshot>,
    pub after: Arc<Snapshot>,
}