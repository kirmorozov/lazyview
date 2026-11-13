//! lazyview: materialized views for MySQL-family servers, maintained from the binlog.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

pub mod access;
pub mod analyze;
pub mod analyzer;
pub mod binlog;
pub mod cascade;
pub mod catalog;
pub mod config;
pub mod coord;
pub mod datum;
pub mod ddl;
pub mod dedup;
pub mod engine;
pub mod exec;
pub mod fence;
pub mod health;
pub mod intset;
pub mod lookback;
pub mod metrics;
pub mod pgoutput;
pub mod stats;

pub use config::Config;
pub use metrics::{Metrics, Snapshot};
pub mod accumulate;
pub mod meta;
pub mod mv;
pub mod plan_swap;
pub mod supervisor;
pub mod target;
