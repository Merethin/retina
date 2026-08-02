use crate::graphql::{Nation, Region};

#[derive(Clone)]
pub struct DelegateChangeEvent {
    pub name: String,
    pub region: Region,
}

#[derive(Clone)]
pub struct RegionChangeEvent {
    pub name: String,
    pub region: Region,
}

#[derive(Clone)]
pub struct NationChangeEvent {
    pub name: String,
    pub nation: Nation,
}

#[derive(Clone)]
pub struct EndoChangeEvent {
    pub name: String,
    pub nation: Nation,
}

#[derive(Clone)]
pub struct NationCreateEvent {
    pub nation: Nation,
}

#[derive(Clone)]
pub struct NationDeleteEvent {
    pub nation: Nation,
}

#[derive(Clone)]
pub struct NationMoveEvent {
    pub name: String,
    pub nation: Nation,
}

#[derive(Clone)]
pub struct RegionUpdateEvent {
    pub name: String,
    pub region: Region,
}

#[derive(Clone)]
pub struct RegionDeleteEvent {
    pub name: String,
}

#[derive(Clone)]
pub struct WAChangeEvent {
    pub name: String,
    pub nation: Nation,
}

#[derive(Clone)]
pub struct BootstrapEvent {
    pub last_id: i64,
}

#[derive(Clone)]
pub enum SubscriptionEvent {
    DelegateChange(DelegateChangeEvent),
    RegionChange(RegionChangeEvent),
    NationChange(NationChangeEvent),
    EndoChange(EndoChangeEvent),
    NationCreate(NationCreateEvent),
    NationDelete(NationDeleteEvent),
    NationMove(NationMoveEvent),
    RegionUpdate(RegionUpdateEvent),
    RegionDelete(RegionDeleteEvent),
    WAChange(WAChangeEvent),
    Bootstrap(BootstrapEvent),
}