// Copyright © Aptos Foundation

use std::{error::Error, io::Write};

use chrono::Utc;
use image::{
    codecs::jpeg,
    imageops::{resize, FilterType},
    ColorType, ImageFormat,
};
use std::fs::File;

use serde_json::Value;

use crate::{
    db::upsert_uris,
    establish_connection,
    models::{NFTMetadataCrawlerEntry, NFTMetadataCrawlerURIs},
};

pub struct Parser {
    pub entry: NFTMetadataCrawlerEntry,
    model: NFTMetadataCrawlerURIs,
    format: ImageFormat,
    target_size: (u32, u32),
}

impl Parser {
    pub fn new(e: NFTMetadataCrawlerEntry, ts: Option<(u32, u32)>) -> Self {
        Self {
            model: NFTMetadataCrawlerURIs {
                token_uri: e.token_uri.clone(),
                raw_image_uri: None,
                cdn_json_uri: None,
                cdn_image_uri: None,
                image_resizer_retry_count: 0,
                json_parser_retry_count: 0,
                last_updated: Utc::now().naive_utc(),
            },
            entry: e,
            format: ImageFormat::Jpeg,
            target_size: ts.unwrap_or((400, 400)),
        }
    }

    pub async fn parse(&mut self) -> Result<(), Box<dyn Error>> {
        let conn = &mut establish_connection();
        match self.parse_json().await {
            Ok(json) => {
                println!("Successfully parsed {}", self.entry.token_uri);
                match self.write_json_to_gcs(json) {
                    Ok(_) => println!("Successfully saved JSON"),
                    Err(_) => println!("Error saving JSON, {}", self.entry.token_uri),
                }
            },
            Err(_) => {
                self.model.json_parser_retry_count += 1;
                println!("Error parsing {}", self.entry.token_uri)
            },
        }
        upsert_uris(conn, self.model.clone());

        match self.optimize_image().await {
            Ok(new_img) => {
                println!("Successfully optimized image");
                match self.write_image_to_gcs(new_img) {
                    Ok(_) => println!("Successfully saved image"),
                    Err(_) => println!("Error saving image {}", self.entry.token_uri),
                }
            },
            Err(_) => {
                self.model.image_resizer_retry_count += 1;
                println!("Error optimizing image {}", self.entry.token_uri)
            },
        }
        upsert_uris(conn, self.model.clone());
        Ok(())
    }

    async fn parse_json(&mut self) -> Result<Value, Box<dyn Error>> {
        for _ in 0..3 {
            println!("Sending request {}", self.entry.token_uri);
            let response = reqwest::get(&self.entry.token_uri).await?;
            if response.status().is_success() {
                let parsed_json: Value = response.json().await?;
                if let Some(img) = parsed_json["image"].as_str() {
                    self.model.raw_image_uri = Some(img.to_string());
                    self.model.last_updated = Utc::now().naive_local();
                    return Ok(parsed_json);
                }
            }
        }
        Err("Error sending request".into())
    }

    fn write_json_to_gcs(&mut self, json: Value) -> Result<(), Box<dyn Error>> {
        let json_string = json.to_string();
        let json_loc = format!("./CDN/json_{}.json", self.entry.token_data_id);
        let mut file = File::create(&json_loc)?;
        file.write_all(json_string.as_bytes())?;
        self.model.cdn_json_uri = Some(json_loc);
        Ok(())
    }

    async fn optimize_image(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        for _ in 0..3 {
            let img_uri_option = self
                .model
                .raw_image_uri
                .clone()
                .or_else(|| Some(self.model.token_uri.clone()));
            if let Some(img_uri) = img_uri_option {
                let response = reqwest::get(img_uri.clone()).await?;
                if response.status().is_success() {
                    let img_bytes = response.bytes().await?;
                    self.model.raw_image_uri = Some(img_uri);
                    if let Ok(format) = image::guess_format(img_bytes.as_ref()) {
                        self.format = format;
                        match format {
                            ImageFormat::Gif | ImageFormat::Avif => return Ok(img_bytes.to_vec()),
                            _ => {
                                let img = image::load_from_memory(&img_bytes)?.to_rgb8();
                                return Ok(resize(
                                    &img,
                                    self.target_size.0 as u32,
                                    self.target_size.1 as u32,
                                    FilterType::Gaussian,
                                )
                                .into_raw());
                            },
                        }
                    }
                }
            }
        }
        Err("Error sending request".into())
    }

    fn write_image_to_gcs(&mut self, new_img: Vec<u8>) -> Result<(), Box<dyn Error>> {
        match self.format {
            ImageFormat::Gif | ImageFormat::Avif => {
                let mut out = File::create(format!(
                    "./CDN/image_{}.{}",
                    self.entry.token_data_id,
                    self.format
                        .extensions_str()
                        .get(0)
                        .unwrap_or(&"gif")
                        .to_string()
                ))?;
                out.write_all(&new_img)?;
            },
            _ => jpeg::JpegEncoder::new(&mut File::create(format!(
                "./CDN/image_{}.jpg",
                self.entry.token_data_id
            ))?)
            .encode(
                &new_img,
                self.target_size.0,
                self.target_size.1,
                ColorType::Rgb8,
            )?,
        };

        Ok(())
    }
}
