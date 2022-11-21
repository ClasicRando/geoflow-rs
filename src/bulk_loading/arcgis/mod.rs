pub mod metadata;
pub mod scraping;

use self::{
    metadata::{ArcGisRestMetadata, RestServiceFieldType, ServiceField},
    scraping::fetch_query,
};
use super::load::csv_values_to_string;
use crate::bulk_loading::{
    analyze::{Schema, SchemaParser},
    error::BulkDataResult,
    load::{DataLoader, DataParser},
    options::DataFileOptions,
};
use chrono::{Utc, TimeZone};
use geo_types::Geometry;
use reqwest::Url;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::mpsc::{error::SendError, Sender};
use wkt::ToWkt;

pub struct ArcGisDataOptions {
    url: Url,
}

impl DataFileOptions for ArcGisDataOptions {}

impl ArcGisDataOptions {
    pub fn new(url: &str) -> BulkDataResult<Self> {
        let url = match Url::parse(url) {
            Ok(url) => url,
            Err(error) => return Err(format!("Url parsing error. {}", error).into()),
        };
        Ok(Self { url })
    }

    async fn metadata(&self) -> BulkDataResult<ArcGisRestMetadata> {
        ArcGisRestMetadata::from_url(&self.url).await
    }
}

pub struct ArcGisRestSchemaParser(ArcGisDataOptions);

#[async_trait::async_trait]
impl SchemaParser for ArcGisRestSchemaParser {
    type Options = ArcGisDataOptions;
    type DataParser = ArcGisRestParser;

    fn new(options: Self::Options) -> Self
    where
        Self: Sized,
    {
        Self(options)
    }

    async fn schema(&self) -> BulkDataResult<Schema> {
        let metadata = self.0.metadata().await?;
        metadata.try_into()
    }

    fn data_loader(self) -> DataLoader<Self::DataParser> {
        let options = self.0;
        let parser = ArcGisRestParser::new(options);
        DataLoader::new(parser)
    }
}

fn map_arcgis_value(value: &Value, field: &ServiceField) -> BulkDataResult<String> {
    Ok(match field.field_type() {
        RestServiceFieldType::Blob => return Err("Blob type fields are not supported".into()),
        RestServiceFieldType::Geometry => return Err("Geometry type fields are not supported".into()),
        RestServiceFieldType::Raster => return Err("Raster type fields are not supported".into()),
        RestServiceFieldType::Date => {
            match value {
                Value::Null => String::new(),
                Value::Number(n) => {
                    let Some(milliseconds) = n.as_i64() else {
                        return Err(format!("Number of {} cannot be converted to i64", n).into())
                    };
                    let dt = Utc.timestamp_millis(milliseconds);
                    format!("{}", dt.format("%Y-%m-%d %H:%M:%S"))
                }
                _ => return Err("Date fields should only contain numbers or null".into()),
            }
        }
        _ => super::geo_json::map_json_value(value),
    })
}

pub struct ArcGisRestParser(ArcGisDataOptions);

impl ArcGisRestParser {
    pub fn new(options: ArcGisDataOptions) -> Self {
        Self(options)
    }
}

#[async_trait::async_trait]
impl DataParser for ArcGisRestParser {
    type Options = ArcGisDataOptions;

    fn options(&self) -> &Self::Options {
        &self.0
    }

    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let options = self.0;
        let metadata = match options.metadata().await {
            Ok(m) => m,
            Err(error) => return record_channel.send(Err(error)).await.err(),
        };
        let query_format = metadata.query_format();
        let fields: HashMap<String, &ServiceField> = metadata
            .fields()
            .map(|f| (f.name().to_owned(), f))
            .collect();
        let queries = match metadata.queries() {
            Ok(q) => q,
            Err(error) => return record_channel.send(Err(error)).await.err(),
        };
        let client = reqwest::Client::new();
        for query in queries {
            let query = match query {
                Ok(q) => q,
                Err(error) => return record_channel.send(Err(error)).await.err(),
            };
            let feature_collection = match fetch_query(&client, &query, query_format).await {
                Ok(c) => c,
                Err(error) => return record_channel.send(Err(error)).await.err(),
            };
            for feature in feature_collection {
                let geom = feature
                    .geometry
                    .as_ref()
                    .and_then(|g| Geometry::<f64>::try_from(g).ok())
                    .map(|g| g.wkt_string())
                    .unwrap_or_default();
                let collect_result = feature
                    .properties_iter()
                    .map(|(key, value)| -> BulkDataResult<String> {
                        let Some(field) = fields.get(key) else {
                            return Err(format!("Could not find a key found in a feature's properties: \"{}\"", key).into())
                        };
                        map_arcgis_value(value, field)
                    })
                    .collect::<BulkDataResult<_>>();
                let mut csv_row: Vec<String> = match collect_result {
                    Ok(inner) => inner,
                    Err(error) => return record_channel.send(Err(error)).await.err(),
                };
                csv_row.push(geom);
                let result = record_channel.send(Ok(csv_values_to_string(csv_row))).await;
                if let Err(error) = result {
                    return Some(error);
                }
            }
        }
        None
    }
}
