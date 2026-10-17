//! Leadership between lazyview instances working on the same schema (hot standby), with a
//! fencing epoch so a deposed leader can never persist progress.
//!
//! Default backend: MySQL named locks (`GET_LOCK`) on the metadata connection — no extra
//! infrastructure. The lock lives as long as that connection; if it drops, a standby takes over.
//! The epoch (`_lazyview_status.epoch`, incremented on every takeover) is checked by every
//! position write, so an instance that lost the lock stops at its next flush.
//! A ZooKeeper / ClickHouse Keeper backend can implement the same contract (ephemeral node +
//! epoch from the node's version) without touching the engine.

use std::time::Duration;

use anyhow::{bail, Result};
use mysql_async::prelude::*;
use mysql_async::Conn;
use tokio::sync::watch;
use tracing::info;

use crate::metrics::Metrics;

pub struct Leadership {
    pub lock_name: String,
    pub epoch: u64,
}

pub fn lock_name(schema: &str, instance_id: &str) -> String {
    format!("lazyview:{}", crate::analyze::definition_hash(&format!("{schema}/{instance_id}")))
}

/// Blocks (as hot standby) until this process holds the lock, then bumps the fencing epoch.
/// `Ok(None)` when shutdown was requested while waiting.
pub async fn acquire(conn: &mut Conn, schema: &str, instance_id: &str, metrics: &Metrics, shutdown: &mut watch::Receiver<bool>) -> Result<Option<Leadership>> {
    let name = lock_name(schema, instance_id);
    let mut logged = false;
    let mut wait = Duration::from_millis(250);
    loop {
        let got: Option<Option<i64>> = conn.exec_first("SELECT GET_LOCK(?, 0)", (&name,)).await?;
        if got.flatten() == Some(1) {
            break;
        }
        if !logged {
            info!("another lazyview instance (LAZYVIEW_INSTANCE_ID={instance_id}) is active on schema `{schema}`; standing by and taking over if it stops");
            logged = true;
        }
        Metrics::set(&metrics.gauges.standby, 1);
        metrics.beat();
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.changed() => if *shutdown.borrow() { return Ok(None) },
        }
        wait = (wait * 2).min(Duration::from_secs(5));
    }
    Metrics::set(&metrics.gauges.standby, 0);
    if logged {
        info!("took over as active instance");
    }
    conn.exec_drop(
        "INSERT INTO `_lazyview_status` (`instance_id`, `binlog_file`, `binlog_pos`, `epoch`) VALUES (?, '', 0, 1) \
         ON DUPLICATE KEY UPDATE `epoch` = `epoch` + 1",
        (instance_id,),
    )
    .await?;
    let epoch: Option<u64> = conn
        .exec_first("SELECT `epoch` FROM `_lazyview_status` WHERE `instance_id` = ?", (instance_id,))
        .await?;
    Ok(Some(Leadership { lock_name: name, epoch: epoch.unwrap_or(1) }))
}

/// Fails if this connection no longer holds the lock (e.g. it was killed and re-established).
pub async fn verify(conn: &mut Conn, l: &Leadership) -> Result<()> {
    let ok: Option<Option<i64>> = conn
        .exec_first("SELECT IS_USED_LOCK(?) = CONNECTION_ID()", (&l.lock_name,))
        .await?;
    if ok.flatten() != Some(1) {
        bail!("lost leadership lock `{}`; another instance may be active", l.lock_name);
    }
    Ok(())
}
