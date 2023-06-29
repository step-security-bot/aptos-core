// Copyright © Aptos Foundation

use crate::{models::NFTMetadataCrawlerURIs, schema};

use diesel::{upsert::excluded, ExpressionMethods, PgConnection, RunQueryDsl, SelectableHelper};

use crate::models::NFTMetadataCrawlerEntry;

pub fn upsert_entry(
    conn: &mut PgConnection,
    entry: NFTMetadataCrawlerEntry,
) -> NFTMetadataCrawlerEntry {
    use schema::nft_metadata_crawler_entry::dsl::*;

    diesel::insert_into(schema::nft_metadata_crawler_entry::table)
        .values(&entry)
        .on_conflict(token_data_id)
        .do_update()
        .set((
            token_uri.eq(excluded(token_uri)),
            retry_count.eq(excluded(retry_count)),
            last_transaction_version.eq(excluded(last_transaction_version)),
            last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
            last_updated.eq(excluded(last_updated)),
        ))
        .returning(NFTMetadataCrawlerEntry::as_returning())
        .get_result(conn)
        .expect("Error upserting entry")
}

pub fn upsert_uris(
    conn: &mut PgConnection,
    entry: NFTMetadataCrawlerURIs,
) -> NFTMetadataCrawlerURIs {
    use schema::nft_metadata_crawler_uris::dsl::*;

    diesel::insert_into(schema::nft_metadata_crawler_uris::table)
        .values(&entry)
        .on_conflict(token_uri)
        .do_update()
        .set((
            raw_image_uri.eq(excluded(raw_image_uri)),
            cdn_json_uri.eq(excluded(cdn_json_uri)),
            cdn_image_uri.eq(excluded(cdn_image_uri)),
            image_resizer_retry_count.eq(excluded(image_resizer_retry_count)),
            json_parser_retry_count.eq(excluded(json_parser_retry_count)),
            last_updated.eq(excluded(last_updated)),
        ))
        .returning(NFTMetadataCrawlerURIs::as_returning())
        .get_result(conn)
        .expect("Error upserting URIs")
}
