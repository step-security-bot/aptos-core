// Copyright © Aptos Foundation

use std::{
    env,
    io::{self},
};

use ::futures::future;
use chrono::{NaiveDateTime, Utc};
use diesel::PgConnection;
use nft_metadata_crawler_parser::{
    db::upsert_entry, establish_connection, models::NFTMetadataCrawlerEntry, parser::Parser,
};
use nft_metadata_crawler_utils::consume_from_queue;
use reqwest::Client;
use serde::Deserialize;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Deserialize)]
pub struct URIEntry {
    token_data_id: String,
    token_uri: String,
    last_transaction_version: String,
    last_transaction_timestamp: String,
}

impl URIEntry {
    fn from_str(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() == 4 {
            Some(URIEntry {
                token_data_id: parts[0].to_string(),
                token_uri: parts[1].to_string(),
                last_transaction_version: parts[2].to_string(),
                last_transaction_timestamp: parts[3].to_string(),
            })
        } else {
            None
        }
    }
}

async fn process_response(res: Vec<String>, conn: &mut PgConnection) -> io::Result<()> {
    // let mut rdr = ReaderBuilder::new()
    //     .has_headers(false)
    //     .from_reader(res.as_bytes());

    // let mut uris = Vec::new();
    // for result in rdr.deserialize() {
    //     let record: URIEntry = result?;
    //     uris.push(process_record(record, conn).await?);
    // }
    let entries: Vec<URIEntry> = res
        .into_iter()
        .filter_map(|s| URIEntry::from_str(&s))
        .collect();

    let mut uris: Vec<NFTMetadataCrawlerEntry> = Vec::new();
    for entry in entries {
        uris.push(process_record(entry, conn).await?);
    }

    let handles: Vec<_> = uris.into_iter().map(spawn_parser).collect();
    future::join_all(handles).await;
    Ok(())
}

async fn process_record(
    record: URIEntry,
    conn: &mut PgConnection,
) -> Result<NFTMetadataCrawlerEntry, csv::Error> {
    let last_transaction_version = record
        .last_transaction_version
        .parse()
        .expect("Error parsing last_transaction_version");
    let last_transaction_timestamp =
        NaiveDateTime::parse_from_str(&record.last_transaction_timestamp, "%Y-%m-%d %H:%M:%S %Z")
            .expect("Error parsing last_transaction_timestamp");

    Ok(upsert_entry(
        &mut *conn,
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

#[tokio::main]
async fn main() {
    let mut conn = establish_connection();
    let client = Client::new();
    let auth = env::var("AUTH").expect("No AUTH");
    let subscription_name = env::var("SUBSCRIPTION_NAME").expect("No SUBSCRIPTION NAME");

    if let Ok(res) = consume_from_queue(&client, &auth, &subscription_name).await {
        match process_response(res, &mut conn).await {
            Ok(_) => println!("Successfully processed response"),
            Err(e) => println!("Error processing response: {}", e),
        };
    } else {
        println!("Error processing response");
    }
}
