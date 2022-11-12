use shapefile::{dbase::FieldValue, Reader, Shape};
use std::{fs::File, io::BufReader, path::PathBuf};
use tokio::sync::mpsc::{error::SendError, Sender};
use wkt::ToWkt;

use super::{
    error::BulkDataResult,
    load::{csv_iter_to_string, DataParser},
    options::DataFileOptions, analyze::{SchemaParser, Schema, ColumnType, ColumnMetadata},
};

pub struct ShapeDataOptions {
    file_path: PathBuf,
}

impl ShapeDataOptions {
    pub fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }

    fn reader(&self) -> BulkDataResult<Reader<BufReader<File>>> {
        let reader = Reader::from_path(&self.file_path)?;
        Ok(reader)
    }
}

impl DataFileOptions for ShapeDataOptions {}

fn column_type_from_value(value: FieldValue) -> ColumnType {
    match value {
        FieldValue::Character(_) => ColumnType::Text,
        FieldValue::Numeric(_) => ColumnType::Number,
        FieldValue::Logical(_) => ColumnType::Boolean,
        FieldValue::Date(_) => ColumnType::Date,
        FieldValue::Float(_) => ColumnType::Real,
        FieldValue::Integer(_) => ColumnType::Integer,
        FieldValue::Currency(_) => ColumnType::Money,
        FieldValue::DateTime(_) => ColumnType::Timestamp,
        FieldValue::Double(_) => ColumnType::DoublePrecision,
        FieldValue::Memo(_) => ColumnType::Text,
    }
}

pub struct ShapeDataSchemaParser(ShapeDataOptions);

impl SchemaParser for ShapeDataSchemaParser {
    type Options = ShapeDataOptions;

    fn new(options: ShapeDataOptions) -> Self
    where
        Self: Sized,
    {
        Self(options)
    }

    fn schema(&self) -> BulkDataResult<Schema> {
        let Some(table_name) = self.0.file_path.file_name().and_then(|f| f.to_str()) else {
            return Err(format!("Could not get filename for \"{:?}\"", &self.0.file_path).into())
        };
        let mut feature_reader = self.0.reader()?;
        let Some(Ok((_, record))) = feature_reader.iter_shapes_and_records().next() else {
            return Err(format!("Could not get the first feature for \"{:?}\"", &self.0.file_path).into())
        };
        let mut columns: Vec<ColumnMetadata> = record
            .into_iter()
            .enumerate()
            .map(|(index, (field, value))| {
                ColumnMetadata::new(&field, index, column_type_from_value(value))
            })
            .collect::<BulkDataResult<_>>()?;
        columns.push(ColumnMetadata::new("geometry", columns.len(), ColumnType::Geometry)?);
        Schema::new(table_name, columns)
    }
}

fn map_field_value(value: FieldValue) -> String {
    match value {
        FieldValue::Character(str) => str.unwrap_or_default(),
        FieldValue::Numeric(n) => n.map(|f| f.to_string()).unwrap_or_default(),
        FieldValue::Logical(l) => l.map(|b| b.to_string()).unwrap_or_default(),
        FieldValue::Date(date) => date
            .map(|d| format!("{}-{:02}-{:02}", d.year(), d.month(), d.day()))
            .unwrap_or_default(),
        FieldValue::Float(f) => f.map(|f| f.to_string()).unwrap_or("".into()),
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Currency(c) => format!("${}", c),
        FieldValue::DateTime(dt) => {
            let date = dt.date();
            let time = dt.time();
            format!(
                "{}-{:02}-{:02} {}:{:02}:{:02}",
                date.year(),
                date.month(),
                date.day(),
                time.hours(),
                time.minutes(),
                time.seconds()
            )
        }
        FieldValue::Double(d) => d.to_string(),
        FieldValue::Memo(m) => m,
    }
}

pub struct ShapeDataParser(ShapeDataOptions);

impl ShapeDataParser {
    pub fn new(options: ShapeDataOptions) -> Self {
        Self(options)
    }
}

#[async_trait::async_trait]
impl DataParser for ShapeDataParser {
    type Options = ShapeDataOptions;

    fn options(&self) -> &Self::Options {
        &self.0
    }

    async fn spool_records(
        mut self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let options = self.0;
        let mut reader = match options.reader() {
            Ok(reader) => reader,
            Err(error) => return record_channel.send(Err(error)).await.err(),
        };
        for (feature_number, feature) in reader.iter_shapes_and_records().enumerate() {
            let Ok((shape, record)) = feature else {
                return record_channel
                    .send(Err(format!("Could not obtain feature {}", &feature_number).into()))
                    .await
                    .err();
            };
            let wkt = match shape {
                Shape::NullShape => String::new(),
                _ => {
                    let Ok(geo) = geo_types::Geometry::<f64>::try_from(shape) else {
                        return record_channel
                            .send(Err(format!("Could not obtain shape for feature {}", &feature_number).into()))
                            .await
                            .err();
                    };
                    geo.wkt_string()
                }
            };
            let csv_iter = record
                .into_iter()
                .map(|(_, value)| map_field_value(value))
                .chain(std::iter::once(wkt));
            let result = record_channel.send(Ok(csv_iter_to_string(csv_iter))).await;
            if let Err(error) = result {
                return Some(error);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shapefile::dbase::{Date, DateTime, Time};

    #[test]
    fn map_field_value_should_return_exact_string_when_character_some() {
        let value = FieldValue::Character(Some(String::from("This is a test")));

        let actual = map_field_value(value);

        assert_eq!("This is a test", actual);
    }

    #[test]
    fn map_field_value_should_return_empty_string_when_character_none() {
        let value = FieldValue::Character(None);

        let actual = map_field_value(value);

        assert_eq!("", actual);
    }

    #[test]
    fn map_field_value_should_return_literal_number_when_numeric_some() {
        let value = FieldValue::Numeric(Some(12.5));

        let actual = map_field_value(value);

        assert_eq!("12.5", actual);
    }

    #[test]
    fn map_field_value_should_return_empty_string_when_numeric_none() {
        let value = FieldValue::Numeric(None);

        let actual = map_field_value(value);

        assert_eq!("", actual);
    }

    #[test]
    fn map_field_value_should_return_literal_bool_when_logical_some_true() {
        let value = FieldValue::Logical(Some(true));

        let actual = map_field_value(value);

        assert_eq!("true", actual);
    }

    #[test]
    fn map_field_value_should_return_literal_bool_when_logical_some_false() {
        let value = FieldValue::Logical(Some(false));

        let actual = map_field_value(value);

        assert_eq!("false", actual);
    }

    #[test]
    fn map_field_value_should_return_empty_string_when_logical_none() {
        let value = FieldValue::Logical(None);

        let actual = map_field_value(value);

        assert_eq!("", actual);
    }

    #[test]
    fn map_field_value_should_return_date_string_when_date_some() {
        let date = Date::new(1, 1, 2000);
        let value = FieldValue::Date(Some(date));

        let actual = map_field_value(value);

        assert_eq!("2000-01-01", actual);
    }

    #[test]
    fn map_field_value_should_return_empty_string_when_date_none() {
        let value = FieldValue::Date(None);

        let actual = map_field_value(value);

        assert_eq!("", actual);
    }

    #[test]
    fn map_field_value_should_return_float_string_when_float_some() {
        let value = FieldValue::Float(Some(29.526));

        let actual = map_field_value(value);

        assert_eq!("29.526", actual);
    }

    #[test]
    fn map_field_value_should_return_empty_string_when_float_none() {
        let value = FieldValue::Float(None);

        let actual = map_field_value(value);

        assert_eq!("", actual);
    }

    #[test]
    fn map_field_value_should_return_integer_string_when_integer() {
        let value = FieldValue::Integer(25386);

        let actual = map_field_value(value);

        assert_eq!("25386", actual);
    }

    #[test]
    fn map_field_value_should_return_currency_string_when_currency() {
        let value = FieldValue::Currency(56.98);

        let actual = map_field_value(value);

        assert_eq!("$56.98", actual);
    }

    #[test]
    fn map_field_value_should_return_timestamp_string_when_datetime() {
        let date = Date::new(1, 1, 2000);
        let time = Time::new(13, 6, 57);
        let datetime = DateTime::new(date, time);
        let value = FieldValue::DateTime(datetime);

        let actual = map_field_value(value);

        assert_eq!("2000-01-01 13:06:57", actual);
    }

    #[test]
    fn map_field_value_should_return_double_string_when_double() {
        let value = FieldValue::Double(48.2356);

        let actual = map_field_value(value);

        assert_eq!("48.2356", actual);
    }

    #[test]
    fn map_field_value_should_return_exact_string_when_memo() {
        let value = FieldValue::Memo(String::from("This is a test"));

        let actual = map_field_value(value);

        assert_eq!("This is a test", actual);
    }
}
