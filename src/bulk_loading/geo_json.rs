use geo_types::Geometry;
use geojson::{Feature, FeatureReader, JsonValue};
use serde_json::{Map, Value};
use std::{fs::File, io::BufReader, path::PathBuf};
use tokio::sync::mpsc::{error::SendError, Sender};
use wkt::ToWkt;

use super::{
    analyze::{ColumnType, Schema, SchemaParser},
    error::BulkDataResult,
    load::{csv_iter_to_string, DataLoader, DataParser},
    options::DataFileOptions,
    utilities::send_error_message,
};

fn column_type_from_value(value: &JsonValue) -> Option<ColumnType> {
    match value {
        JsonValue::Null => None,
        JsonValue::Bool(_) => Some(ColumnType::Boolean),
        JsonValue::Number(_) => Some(ColumnType::Number),
        JsonValue::String(_) => Some(ColumnType::Text),
        JsonValue::Array(_) => Some(ColumnType::Json),
        JsonValue::Object(_) => Some(ColumnType::Json),
    }
}

fn collect_columns_into_schema(
    table_name: &str,
    columns: Vec<(String, Option<ColumnType>)>,
) -> BulkDataResult<Schema> {
    let columns = columns
        .into_iter()
        .map(|(field, typ)| (field, typ.unwrap_or(ColumnType::Text)))
        .chain(std::iter::once((
            String::from("geometry"),
            ColumnType::Geometry,
        )));
    Schema::from_iter(table_name, columns)
}

pub struct GeoJsonOptions {
    file_path: PathBuf,
}

impl GeoJsonOptions {
    pub fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }

    fn reader(&self) -> BulkDataResult<FeatureReader<BufReader<File>>> {
        let file = File::open(&self.file_path)?;
        let buff_reader = BufReader::new(file);
        Ok(FeatureReader::from_reader(buff_reader))
    }
}

impl DataFileOptions for GeoJsonOptions {}

pub struct GeoJsonSchemaParser(GeoJsonOptions);

#[async_trait::async_trait]
impl SchemaParser for GeoJsonSchemaParser {
    type Options = GeoJsonOptions;
    type DataParser = GeoJsonParser;

    fn new(options: GeoJsonOptions) -> Self
    where
        Self: Sized,
    {
        Self(options)
    }

    async fn schema(&self) -> BulkDataResult<Schema> {
        let Some(table_name) = self.0.file_path.file_name().and_then(|f| f.to_str()) else {
            return Err(format!("Could not get filename for \"{:?}\"", &self.0.file_path).into())
        };
        let feature_reader = self.0.reader()?;
        let mut undefined_type = false;
        let mut features = feature_reader.features();
        let first_feature = match features.next() {
            Some(Ok(f)) => f,
            Some(Err(error)) => return Err(error.into()),
            None => return Schema::new(table_name, vec![]),
        };
        let mut columns: Vec<(String, Option<ColumnType>)> = first_feature
            .properties_iter()
            .map(|(field, value)| {
                let typ = column_type_from_value(value);
                undefined_type = undefined_type || typ.is_none();
                (field.to_owned(), typ)
            })
            .collect();

        if !undefined_type {
            return collect_columns_into_schema(table_name, columns);
        }

        for feature in features {
            let feature = feature?;
            for (i, (field, value)) in feature.properties_iter().enumerate() {
                match columns.get_mut(i) {
                    Some((_, Some(_))) => continue,
                    Some((_, typ)) => {
                        *typ = column_type_from_value(value);
                        undefined_type = undefined_type || typ.is_none();
                    }
                    None => return Err(
                        format!(
                            "Found column with index {} named \"{}\" that was not found in the first feature",
                            i,
                            field
                        )
                        .into()
                    )
                }
            }
            if !undefined_type {
                break;
            }
            undefined_type = false;
        }
        collect_columns_into_schema(table_name, columns)
    }

    fn data_loader(self) -> DataLoader<Self::DataParser> {
        let options = self.0;
        let parser = GeoJsonParser::new(options);
        DataLoader::new(parser)
    }
}

pub fn map_json_value(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => String::from(""),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => format!("{}", n),
        JsonValue::String(s) => s.to_owned(),
        _ => format!("{}", value),
    }
}

#[inline]
pub fn feature_geometry_as_wkt(feature: &Feature) -> BulkDataResult<String> {
    let Some(ref geom) = feature.geometry else {
        return Ok(String::new())
    };
    match Geometry::<f64>::try_from(geom) {
        Ok(g) => Ok(g.wkt_string()),
        Err(error) => Err(error.into()),
    }
}

fn feature_properties_to_iter(
    properties: &Map<String, Value>,
) -> impl Iterator<Item = String> + '_ {
    properties
        .into_iter()
        .map(|(_, value)| map_json_value(value))
}

pub struct GeoJsonParser(GeoJsonOptions);

impl GeoJsonParser {
    pub fn new(options: GeoJsonOptions) -> Self {
        Self(options)
    }
}

#[async_trait::async_trait]
impl DataParser for GeoJsonParser {
    type Options = GeoJsonOptions;

    fn options(&self) -> &Self::Options {
        &self.0
    }

    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let options = self.0;
        let reader = match options.reader() {
            Ok(r) => r,
            Err(error) => return send_error_message(record_channel, error).await,
        };
        for feature in reader.features() {
            let feature = match feature {
                Ok(f) => f,
                Err(error) => return send_error_message(record_channel, error).await,
            };
            let geom = match feature_geometry_as_wkt(&feature) {
                Ok(g) => g,
                Err(error) => return send_error_message(record_channel, error).await,
            };
            let csv_row = match feature.properties {
                Some(properies) => {
                    let csv_iter =
                        feature_properties_to_iter(&properies).chain(std::iter::once(geom));
                    csv_iter_to_string(csv_iter)
                }
                None => String::new(),
            };
            let result = record_channel.send(Ok(csv_row)).await;
            if let Err(error) = result {
                return Some(error);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use geojson::JsonValue;
    use rocket::serde::json::serde_json::json;

    use super::*;

    #[test]
    fn json_value_formatting_array() {
        let value = json!(["This", "is", "a", "test"]);

        let actual = map_json_value(&value);

        assert_eq!("[\"This\",\"is\",\"a\",\"test\"]", actual);
    }

    #[test]
    fn json_value_formatting_bool_true() {
        let value = json!(true);

        let actual = map_json_value(&value);

        assert_eq!("true", actual);
    }

    #[test]
    fn json_value_formatting_bool_false() {
        let value = json!(false);

        let actual = map_json_value(&value);

        assert_eq!("false", actual);
    }

    #[test]
    fn json_value_formatting_object() {
        let value = json!({
            "code": 1,
            "test": true
        });

        let actual = map_json_value(&value);

        assert_eq!("{\"code\":1,\"test\":true}", actual);
    }

    #[test]
    fn json_value_formatting_null() {
        let value = JsonValue::Null;

        let actual = map_json_value(&value);

        assert_eq!("", actual);
    }

    #[test]
    fn json_value_formatting_number() {
        let value = json!(12.5);

        let actual = map_json_value(&value);

        assert_eq!("12.5", actual);
    }

    #[test]
    fn json_value_formatting_string() {
        let expected = "This is a test";
        let value = json!(expected);

        let actual = map_json_value(&value);

        assert_eq!(expected, actual);
    }
}
