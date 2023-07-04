// Copyright © Aptos Foundation

use std::{env, error::Error};

use ::futures::future;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    PgConnection,
};
use nft_metadata_crawler_parser::{
    db::upsert_entry, establish_connection_pool, models::NFTMetadataCrawlerEntry, parser::Parser,
};
use nft_metadata_crawler_utils::consume_from_queue;
use reqwest::Client;
use tokio::task::JoinHandle;

async fn process_response(
    res: Vec<String>,
    pool: &Pool<ConnectionManager<PgConnection>>,
) -> Result<Vec<NFTMetadataCrawlerEntry>, Box<dyn Error>> {
    let mut uris: Vec<NFTMetadataCrawlerEntry> = Vec::new();
    for entry in res {
        uris.push(upsert_entry(
            &mut pool.get()?,
            NFTMetadataCrawlerEntry::new(entry),
        ));
    }
    Ok(uris)
}

fn spawn_parser(
    uri: NFTMetadataCrawlerEntry,
    pool: &Pool<ConnectionManager<PgConnection>>,
) -> JoinHandle<()> {
    match pool.get() {
        Ok(mut conn) => tokio::spawn(async move {
            let mut parser = Parser::new(uri, Some((400, 400)));
            match parser.parse(&mut conn).await {
                Ok(_) => println!("Successfully parsed {}", parser.entry.token_uri),
                Err(_) => println!("Error parsing {}", parser.entry.token_uri),
            }
        }),
        Err(_) => todo!(),
    }
}

#[tokio::main]
async fn main() {
    let pool = establish_connection_pool();
    let client = Client::new();
    let auth = env::var("AUTH").expect("No AUTH");
    let subscription_name = env::var("SUBSCRIPTION_NAME").expect("No SUBSCRIPTION NAME");

    match consume_from_queue(&client, &auth, &subscription_name).await {
        Ok(res) => {
            match process_response(res, &pool).await {
                Ok(uris) => {
                    let handles: Vec<_> = uris
                        .into_iter()
                        .map(|uri| spawn_parser(uri, &pool))
                        .collect();
                    if let Ok(_) = future::try_join_all(handles).await {
                        println!("SUCCESS");
                    }
                },
                Err(e) => println!("Error processing response: {}", e),
            };
        },
        Err(e) => println!("Error consuming from queue: {}", e),
    }
}
