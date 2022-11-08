use shapefile::{dbase::FieldValue, Shape, Reader};
use std::{fs::File, io::BufReader};
use tokio::sync::mpsc::{error::SendError, Sender};
use wkt::ToWkt;

use super::{
    error::BulkDataResult,
    loader::{csv_iter_to_string, DataParser},
    options::DefaultFileOptions,
};

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

pub struct ShapeDataParser {
    options: DefaultFileOptions,
    reader: Reader<BufReader<File>>,
}

#[async_trait::async_trait]
impl DataParser for ShapeDataParser {
    type Options = DefaultFileOptions;

    fn new(options: Self::Options) -> BulkDataResult<Self>
    where
        Self: Sized,
    {
        let reader = Reader::from_path(&options.file_path)?;
        Ok(ShapeDataParser { options, reader })
    }

    fn options(&self) -> &Self::Options {
        &self.options
    }

    async fn spool_records(
        mut self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        for (feature_number, feature) in self.reader.iter_shapes_and_records().enumerate() {
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
    use shapefile::dbase::{DateTime, Date, Time};
    use super::*;

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
