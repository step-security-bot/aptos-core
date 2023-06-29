// Copyright © Aptos Foundation

pub mod db;
pub mod models;
pub mod parser;
pub mod schema;

use diesel::pg::PgConnection;
use diesel::prelude::*;
use std::env;

pub fn establish_connection() -> PgConnection {
    let database_url = env::var("NFT_METADATA_CRAWLER_URL").expect("NFT_METADATA_CRAWLER_URL must be set");
    PgConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}
