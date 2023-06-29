// Copyright © Aptos Foundation

use std::{fs::File, io::BufReader};

use ::futures::future;
use chrono::{NaiveDateTime, Utc};
use diesel::PgConnection;
use nft_metadata_crawler::{
    db::upsert_entry, establish_connection, models::NFTMetadataCrawlerEntry, parser::Parser,
};
use serde::Deserialize;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Deserialize)]
pub struct URIEntry {
    token_data_id: String,
    token_uri: String,
    last_transaction_version: String,
    last_transaction_timestamp: String,
}

#[tokio::main]
async fn main() {
    let conn = &mut establish_connection();
    if let Err(_) = process_file(conn).await {
        println!("Error opening file");
    }
}

async fn process_file(conn: &mut PgConnection) -> std::io::Result<()> {
    let file = File::open("./test.csv")?;
    let reader = BufReader::new(file);
    let mut rdr = csv::Reader::from_reader(reader);

    let uris: Result<Vec<_>, _> = rdr
        .deserialize::<URIEntry>()
        .map(|res| process_record(res, conn))
        .collect();
    let uris = uris?;

    let handles: Vec<_> = uris.into_iter().map(spawn_parser).collect();
    future::join_all(handles).await;
    Ok(())
}

fn process_record(
    res: csv::Result<URIEntry>,
    conn: &mut PgConnection,
) -> Result<NFTMetadataCrawlerEntry, csv::Error> {
    let record = res?;
    let last_transaction_version = record
        .last_transaction_version
        .parse()
        .expect("Error parsing last_transaction_version");
    let last_transaction_timestamp =
        NaiveDateTime::parse_from_str(&record.last_transaction_timestamp, "%Y-%m-%d %H:%M:%S %Z")
            .expect("Error parsing last_transaction_timestamp");

    Ok(upsert_entry(
        conn,
        NFTMetadataCrawlerEntry {
            token_data_id: record.token_data_id.clone(),
            token_uri: record.token_uri.clone(),
            retry_count: 0,
            last_transaction_version,
            last_transaction_timestamp,
            last_updated: Utc::now().naive_utc(),
        },
    ))
}

fn spawn_parser(uri: NFTMetadataCrawlerEntry) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut parser = Parser::new(uri, Some((400, 400)));
        match parser.parse().await {
            Ok(_) => println!("Successfully parsed {}", parser.entry.token_uri),
            Err(_) => println!("Error parsing {}", parser.entry.token_uri),
        }
    })
}
