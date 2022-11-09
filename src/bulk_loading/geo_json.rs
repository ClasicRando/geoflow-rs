use geo_types::Geometry;
use geojson::{FeatureReader, JsonValue};
use std::{fs::File, io::BufReader};
use tokio::sync::mpsc::{error::SendError, Sender};
use wkt::ToWkt;

use super::{
    analyze::{ColumnMetadata, ColumnType, Schema, SchemaParser},
    error::BulkDataResult,
    load::{csv_values_to_string, DataParser},
    options::DefaultFileOptions,
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

pub struct GeoJsonSchemaParser(DefaultFileOptions);

impl SchemaParser for GeoJsonSchemaParser {
    type Options = DefaultFileOptions;

    fn new(options: Self::Options) -> Self
    where
        Self: Sized,
    {
        Self(options)
    }

    fn schema(&self) -> BulkDataResult<Schema> {
        let Some(table_name) = self.0.file_path.file_name().and_then(|f| f.to_str()) else {
            return Err(format!("Could not get filename for \"{:?}\"", &self.0.file_path).into())
        };
        let file = File::open(&self.0.file_path)?;
        let buff_reader = BufReader::new(file);
        let feature_reader = FeatureReader::from_reader(buff_reader);
        let mut undefined_type = false;
        let mut columns: Vec<(String, usize, Option<ColumnType>)> = Vec::new();
        for feature in feature_reader.features() {
            let feature = feature?;
            for (i, (field, value)) in feature.properties_iter().enumerate() {
                // Get column index, update if in vector and undefined type
                let column = columns.get_mut(i).map(|(_, _, typ)| {
                    if typ.is_none() {
                        *typ = column_type_from_value(value);
                        undefined_type = typ.is_none();
                    }
                });
                // If not in vector, add column entry
                if column.is_none() {
                    let typ = column_type_from_value(value);
                    undefined_type = typ.is_none();
                    columns.push((field.to_owned(), i, typ));
                }
            }
            if !undefined_type {
                break;
            }
        }
        let columns: Vec<ColumnMetadata> = columns
            .into_iter()
            .map(|(field, index, typ)| {
                ColumnMetadata::new(field, index, typ.unwrap_or(ColumnType::Text))
            })
            .collect::<BulkDataResult<_>>()?;
        Schema::new(table_name, columns)
    }
}

fn map_json_value(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => String::from(""),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => format!("{}", n),
        JsonValue::String(s) => s.to_owned(),
        _ => format!("{}", value),
    }
}

pub struct GeoJsonParser {
    options: DefaultFileOptions,
    reader: FeatureReader<BufReader<File>>,
}

#[async_trait::async_trait]
impl DataParser for GeoJsonParser {
    type Options = DefaultFileOptions;

    fn new(options: Self::Options) -> BulkDataResult<Self> {
        let file = File::open(&options.file_path)?;
        let buff_reader = BufReader::new(file);
        let feature_reader = FeatureReader::from_reader(buff_reader);
        Ok(Self {
            options,
            reader: feature_reader,
        })
    }

    fn options(&self) -> &Self::Options {
        &self.options
    }

    async fn spool_records(
        mut self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        for (i, feature) in self.reader.features().enumerate() {
            let Ok(feature) = feature else {
                return record_channel
                    .send(Err(format!("Could not obtain feature {}", &i).into()))
                    .await
                    .err();
            };
            let geom = feature
                .geometry
                .as_ref()
                .and_then(|g| Geometry::<f64>::try_from(g).ok())
                .map(|g| g.wkt_string())
                .unwrap_or_default();
            let mut csv_row: Vec<String> = feature
                .properties_iter()
                .map(|(_, value)| map_json_value(value))
                .collect();
            csv_row.push(geom);
            let csv_data = csv_values_to_string(csv_row);
            let result = record_channel.send(Ok(csv_data)).await;
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
