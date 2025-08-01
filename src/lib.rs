#![allow(deprecated)] // XXX temporary to silence expected warnings
#![deny(unused_mut)]
extern crate byteorder;
extern crate bytes;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate mio;
extern crate snowflake;
#[macro_use]
extern crate zookeeper_derive;

pub use acl::*;
pub use consts::{WatcherType, *};
pub use data::*;
pub use watch::{Watch, WatchedEvent, Watcher};
pub use zookeeper::{ZkResult, ZooKeeper};
pub use zookeeper_ext::ZooKeeperExt;

pub use listeners::Subscription;

mod acl;
mod consts;
mod data;
mod io;
mod listeners;
mod paths;
mod proto;
pub mod recipes;
mod try_io;
mod watch;
mod zookeeper;
mod zookeeper_ext;
