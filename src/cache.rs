use std::{collections::{HashMap, HashSet}, error::Error};
use sqlx::{FromRow, PgPool};

#[derive(Debug)]
pub struct EntityCache {
    regions: HashMap<String, usize>,
    nations: HashSet<String>,
}

#[derive(FromRow)]
struct RegionCount {
    region: String,
    count: i64,
}

impl EntityCache {
    pub fn empty() -> Self {
        Self {
            regions: HashMap::new(),
            nations: HashSet::new()
        }
    }

    pub async fn load(pool: &PgPool) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let rows: Vec<RegionCount> = sqlx::query_as(
            "SELECT region, COUNT(*) AS count FROM retina_nations GROUP BY region"
        ).fetch_all(pool).await?;

        let nations: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM retina_nations"
        ).fetch_all(pool).await?;

        Ok(Self {
            regions: rows.into_iter().map(|r| (r.region, r.count as usize)).collect(),
            nations: nations.into_iter().collect(),
        })
    }

    pub fn add_region(&mut self, region: &str) {
        *self.regions.entry(region.to_string()).or_default() += 1;
    }

    pub fn add_nation(&mut self, nation: &str) {
        self.nations.insert(nation.to_string());
    }

    pub fn remove_region(&mut self, region: &str) {
        if let Some(region) = self.regions.get_mut(region) {
            *region -= 1;
        }
    }

    pub fn remove_nation(&mut self, nation: &str) {
        self.nations.remove(nation);
    }

    pub fn check_region(&self, region: &str) -> bool {
        self.regions.contains_key(region)
    }

    pub fn check_nation(&self, nation: &str) -> bool {
        self.nations.contains(nation)
    }
}