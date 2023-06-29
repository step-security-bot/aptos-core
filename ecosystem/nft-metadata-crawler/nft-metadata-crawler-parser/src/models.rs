// Copyright © Aptos Foundation

use diesel::prelude::*;

#[derive(Clone, Insertable, Queryable, Selectable)]
#[diesel(table_name = crate::schema::nft_metadata_crawler_entry)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NFTMetadataCrawlerEntry {
    pub token_data_id: String,
    pub token_uri: String,
    pub retry_count: i32,
    pub last_transaction_version: i32,
    pub last_transaction_timestamp: chrono::NaiveDateTime,
    pub last_updated: chrono::NaiveDateTime,
}

#[derive(Clone, Insertable, Queryable, Selectable)]
#[diesel(table_name = crate::schema::nft_metadata_crawler_uris)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NFTMetadataCrawlerURIs {
    pub token_uri: String,
    pub raw_image_uri: Option<String>,
    pub cdn_json_uri: Option<String>,
    pub cdn_image_uri: Option<String>,
    pub image_resizer_retry_count: i32,
    pub json_parser_retry_count: i32,
    pub last_updated: chrono::NaiveDateTime,
}
