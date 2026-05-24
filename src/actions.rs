use std::error::Error;
use caramel::types::akari::Event;
use sqlx::PgTransaction;

use crate::cache::EntityCache;

pub async fn handle_admit(
    tx: &mut PgTransaction<'_>,
    cache: &mut EntityCache,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = event.actor.as_ref().unwrap();
    let region = event.origin.as_ref().unwrap();

    sqlx::query(
        "INSERT INTO retina_nations (name, region) 
        VALUES ($1, $2) ON CONFLICT (name) DO UPDATE SET
        region = EXCLUDED.region"
    ).bind(name)
    .bind(region)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "DELETE FROM retina_endorsements WHERE target = $1"
    ).bind(name)
    .execute(&mut **tx)
    .await?;

    cache.add_region(&region);
    cache.add_nation(&name);

    Ok(())
}

pub async fn handle_resign(
    tx: &mut PgTransaction<'_>,
    cache: &mut EntityCache,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = event.actor.as_ref().unwrap();
    let region = event.origin.as_ref().unwrap();

    sqlx::query(
        "DELETE FROM retina_nations WHERE name = $1"
    ).bind(name)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "DELETE FROM retina_endorsements WHERE target = $1"
    ).bind(name)
    .execute(&mut **tx)
    .await?;

    cache.remove_region(&region);
    cache.remove_nation(&name);

    Ok(())
}

pub async fn handle_cte(
    tx: &mut PgTransaction<'_>,
    cache: &mut EntityCache,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = event.receptor.as_ref().unwrap();
    let region = event.origin.as_ref().unwrap();

    if !cache.check_nation(&name) {
        return Ok(());
    }

    sqlx::query(
        "DELETE FROM retina_nations WHERE name = $1"
    ).bind(name)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "DELETE FROM retina_endorsements WHERE target = $1"
    ).bind(name)
    .execute(&mut **tx)
    .await?;

    cache.remove_region(&region);
    cache.remove_nation(&name);

    Ok(())
}

pub async fn handle_endo(
    tx: &mut PgTransaction<'_>,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let endorser = event.actor.as_ref().unwrap();
    let target = event.receptor.as_ref().unwrap();

    sqlx::query(
        "INSERT INTO retina_endorsements (endorser, target) VALUES ($1, $2) ON CONFLICT DO NOTHING"
    ).bind(endorser)
    .bind(target)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn handle_remove_endo(
    tx: &mut PgTransaction<'_>,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let endorser = event.actor.as_ref().unwrap();
    let target = event.receptor.as_ref().unwrap();

    sqlx::query(
        "DELETE FROM retina_endorsements WHERE endorser = $1 AND target = $2"
    ).bind(endorser)
    .bind(target)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn handle_move(
    tx: &mut PgTransaction<'_>,
    cache: &mut EntityCache,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = event.actor.as_ref().unwrap();
    let origin = event.origin.as_ref().unwrap();
    let region = event.destination.as_ref().unwrap();

    if !cache.check_nation(&name) {
        return Ok(());
    }

    sqlx::query(
        "UPDATE retina_nations SET region = $2 WHERE name = $1"
    ).bind(name)
    .bind(region)
    .execute(&mut **tx)
    .await?;

    cache.remove_region(&origin);
    cache.add_region(&region);

    Ok(())
}

pub async fn handle_update(
    tx: &mut PgTransaction<'_>,
    cache: &mut EntityCache,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let region = event.origin.as_ref().unwrap();

    if !cache.check_region(&region) {
        return Ok(());
    }

    sqlx::query(
        "DELETE FROM retina_endorsements e
        USING retina_nations n
        WHERE e.target = n.name AND n.region = $1
        AND NOT EXISTS (
            SELECT 1 FROM retina_nations n2 WHERE n2.name = e.endorser AND n2.region = $1
        )"
    ).bind(region).execute(&mut **tx).await?;

    Ok(())
}

pub async fn handle_new_delegate(
    tx: &mut PgTransaction<'_>,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = event.receptor.as_ref().unwrap();
    let region = event.origin.as_ref().unwrap();

    sqlx::query(
        "UPDATE retina_nations SET delegacy = $2 WHERE name = $1"
    ).bind(name).bind(region).execute(&mut **tx).await?;

    Ok(())
}

pub async fn handle_replaced_delegate(
    tx: &mut PgTransaction<'_>,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = event.receptor.as_ref().unwrap();
    let region = event.origin.as_ref().unwrap();
    let old_del = event.data.get(0).unwrap();

    sqlx::query(
        "UPDATE retina_nations SET delegacy = $2 WHERE name = $1"
    ).bind(name).bind(region).execute(&mut **tx).await?;

    sqlx::query(
        "UPDATE retina_nations SET delegacy = NULL WHERE name = $1 AND delegacy = $2"
    ).bind(old_del).bind(region).execute(&mut **tx).await?;

    Ok(())
}

pub async fn handle_lost_delegate(
    tx: &mut PgTransaction<'_>,
    event: &Event
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let name = event.receptor.as_ref().unwrap();
    let region = event.origin.as_ref().unwrap();

    sqlx::query(
        "UPDATE retina_nations SET delegacy = NULL WHERE name = $1 AND delegacy = $2"
    ).bind(name).bind(region).execute(&mut **tx).await?;

    Ok(())
}