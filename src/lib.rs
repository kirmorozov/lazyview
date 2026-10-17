//! lazyview: materialized views for MySQL-family servers, maintained from the binlog.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

pub mod analyze;
pub mod binlog;
pub mod catalog;
pub mod config;
pub mod coord;
pub mod datum;
pub mod ddl;
pub mod engine;
pub mod health;
pub mod metrics;

pub use config::Config;
pub use metrics::{Metrics, Snapshot};
pub mod accumulate;
pub mod meta;
pub mod mv;
