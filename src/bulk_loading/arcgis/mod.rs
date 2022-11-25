pub mod metadata;
pub mod scraping;

use self::{
    metadata::{ArcGisRestMetadata, RestServiceFieldType, ServiceField},
    scraping::fetch_query,
};
use super::load::csv_result_iter_to_string;
use crate::bulk_loading::{
    analyze::{Schema, SchemaParser},
    error::BulkDataResult,
    geo_json::feature_geometry_as_wkt,
    load::{DataLoader, DataParser, RecordSpoolChannel, RecordSpoolResult},
    options::DataFileOptions,
    utilities::send_error_message,
};
use chrono::{TimeZone, Utc};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(Deserialize, Serialize)]
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
        RestServiceFieldType::Geometry => {
            return Err("Geometry type fields are not supported".into())
        }
        RestServiceFieldType::Raster => return Err("Raster type fields are not supported".into()),
        RestServiceFieldType::Date => match value {
            Value::Null => String::new(),
            Value::Number(n) => {
                let Some(milliseconds) = n.as_i64() else {
                        return Err(format!("Number of {} cannot be converted to i64", n).into())
                    };
                let dt = Utc.timestamp_millis(milliseconds);
                format!("{}", dt.format("%Y-%m-%d %H:%M:%S"))
            }
            _ => return Err("Date fields should only contain numbers or null".into()),
        },
        _ => super::geo_json::map_json_value(value),
    })
}

fn feature_properties_to_iter<'m, 'f: 'm>(
    properties: &'m Map<String, Value>,
    fields: &'f HashMap<String, &'f ServiceField>,
) -> impl Iterator<Item = BulkDataResult<String>> + 'm {
    properties
        .into_iter()
        .map(|(key, value)| {
            let Some(field) = fields.get(key.as_str()) else {
                return Err(format!("Could not find a key found in a feature's properties: \"{}\"", key).into())
            };
            map_arcgis_value(value, field)
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

    async fn spool_records(self, record_channel: &mut RecordSpoolChannel) -> RecordSpoolResult {
        let options = self.0;
        let metadata = match options.metadata().await {
            Ok(m) => m,
            Err(error) => return send_error_message(record_channel, error).await,
        };
        let query_format = metadata.query_format();
        let fields: HashMap<String, &ServiceField> = metadata
            .fields()
            .map(|f| (f.name().to_owned(), f))
            .collect();
        let queries = match metadata.queries() {
            Ok(q) => q,
            Err(error) => return send_error_message(record_channel, error).await,
        };
        let client = reqwest::Client::new();
        for query in queries {
            let query = match query {
                Ok(q) => q,
                Err(error) => return send_error_message(record_channel, error).await,
            };
            let feature_collection = match fetch_query(&client, &query, query_format).await {
                Ok(c) => c,
                Err(error) => return send_error_message(record_channel, error).await,
            };
            for feature in feature_collection {
                let geom = match feature_geometry_as_wkt(&feature) {
                    Ok(g) => g,
                    Err(error) => return send_error_message(record_channel, error).await,
                };
                let csv_row = match feature.properties {
                    Some(properies) => {
                        let csv_iter = feature_properties_to_iter(&properies, &fields)
                            .chain(std::iter::once(Ok(geom)));
                        match csv_result_iter_to_string(csv_iter) {
                            Ok(row) => row,
                            Err(error) => return send_error_message(record_channel, error).await,
                        }
                    }
                    None => String::new(),
                };
                let result = record_channel.send(Ok(csv_row)).await;
                if let Err(error) = result {
                    return Some(error);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use crate::bulk_loading::{arcgis::metadata::ServiceField, error::BulkDataResult};

    use super::{map_arcgis_value, metadata::RestServiceFieldType};

    static FIELD_NAME: &str = "test";

    #[test]
    fn map_arcgis_value_should_fail_when_type_is_blob() {
        let typ = RestServiceFieldType::Blob;
        let field = ServiceField::new(FIELD_NAME, typ);
        let value = Value::Null;

        let result = map_arcgis_value(&value, &field);
        assert!(result.is_err())
    }

    #[test]
    fn map_arcgis_value_should_fail_when_type_is_geometry() {
        let typ = RestServiceFieldType::Geometry;
        let field = ServiceField::new(FIELD_NAME, typ);
        let value = Value::Null;

        let result = map_arcgis_value(&value, &field);
        assert!(result.is_err())
    }

    #[test]
    fn map_arcgis_value_should_fail_when_type_is_raster() {
        let typ = RestServiceFieldType::Raster;
        let field = ServiceField::new(FIELD_NAME, typ);
        let value = Value::Null;

        let result = map_arcgis_value(&value, &field);
        assert!(result.is_err())
    }

    #[test]
    fn map_arcgis_value_should_return_when_type_is_date_and_value_is_number() -> BulkDataResult<()>
    {
        let typ = RestServiceFieldType::Date;
        let field = ServiceField::new(FIELD_NAME, typ);
        let value = Value::Number(1000.into());

        let result = map_arcgis_value(&value, &field)?;
        assert_eq!("1970-01-01 00:00:01", &result);
        Ok(())
    }

    #[test]
    fn map_arcgis_value_should_return_empty_string_when_type_is_date_and_value_is_null(
    ) -> BulkDataResult<()> {
        let typ = RestServiceFieldType::Date;
        let field = ServiceField::new(FIELD_NAME, typ);
        let value = Value::Null;

        let result = map_arcgis_value(&value, &field)?;
        assert_eq!("", &result);
        Ok(())
    }

    #[test]
    fn map_arcgis_value_should_fail_when_type_is_date_and_value_not_number() {
        let typ = RestServiceFieldType::Date;
        let field = ServiceField::new(FIELD_NAME, typ);
        let value = Value::String(String::new());

        let result = map_arcgis_value(&value, &field);
        assert!(result.is_err())
    }
}
