use polars::prelude::{AnyValue, DataFrame, TimeUnit};
use tokio::sync::mpsc::{error::SendError, Sender};

use super::{
    error::BulkDataResult,
    loader::{copy_csv_values, CopyPipe, CopyResult, DataParser, csv_values_to_string},
    options::DefaultFileOptions,
};

pub fn escape_csv_string(csv_string: String) -> String {
    if csv_string
        .chars()
        .any(|c| c == '"' || c == ',' || c == '\n' || c == '\r')
    {
        format!("\"{}\"", csv_string.replace("\"", "\"\""))
    } else {
        csv_string
    }
}

pub fn map_formatted_value(value: AnyValue) -> String {
    match value {
        AnyValue::Null => String::new(),
        AnyValue::Utf8(string) => string.to_owned(),
        AnyValue::Utf8Owned(string) => string,
        AnyValue::Duration(duration, unit) => match unit {
            TimeUnit::Microseconds => format!("{} microsecond", duration),
            TimeUnit::Milliseconds => format!("{} milisecond", duration),
            TimeUnit::Nanoseconds => format!("{:.2} microsecond", duration as f64 / 1000.0_f64),
        },
        _ => format!("{}", value),
    }
}

pub struct DataFrameParser {
    options: DefaultFileOptions,
    dataframe: DataFrame,
}

impl DataFrameParser {
    pub fn set_dataframe(&mut self, dataframe: DataFrame) {
        self.dataframe = dataframe;
    }
}

#[async_trait::async_trait]
impl DataParser for DataFrameParser {
    type Options = DefaultFileOptions;

    fn new(options: Self::Options) -> BulkDataResult<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            options,
            dataframe: DataFrame::empty(),
        })
    }

    fn options(&self) -> &Self::Options {
        &self.options
    }

    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let mut iters = self.dataframe.iter().map(|s| s.iter()).collect::<Vec<_>>();
        for i in 0..self.dataframe.height() {
            let row_data = iters
                .iter_mut()
                .map(|iter| {
                    iter.next()
                        .ok_or("Dataframe value was not found. This should never happen".to_string())
                        .map(|value| map_formatted_value(value))
                })
                .collect::<Result<Vec<String>, _>>();
            let Ok(csv_data) = row_data else {
                return record_channel
                    .send(Err(format!("Could not read record {}", (i + 1)).into()))
                    .await
                    .err();
            };
            let result = record_channel
                .send(Ok(csv_values_to_string(csv_data)))
                .await;
            if let Err(error) = result {
                return Some(error);
            }
        }
        None
    }
}

pub async fn load_dataframe(copy: &mut CopyPipe, dataframe: DataFrame) -> CopyResult {
    let mut iters = dataframe.iter().map(|s| s.iter()).collect::<Vec<_>>();
    for _ in 0..dataframe.height() {
        let row_data = iters
            .iter_mut()
            .map(|iter| {
                iter.next()
                    .ok_or("Dataframe value was not found. This should never happen".to_string())
                    .map(|value| map_formatted_value(value))
            })
            .collect::<Result<Vec<String>, _>>()?;
        copy_csv_values(copy, row_data).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use polars::prelude::TimeUnit;

    use super::*;

    #[test]
    fn map_formatted_value_should_return_true_string_literal_when_boolean_true() {
        let value = AnyValue::Boolean(true);

        let actual = map_formatted_value(value);

        assert_eq!("true", actual);
    }

    #[test]
    fn map_formatted_value_should_return_false_string_literal_when_boolean_false() {
        let value = AnyValue::Boolean(false);

        let actual = map_formatted_value(value);

        assert_eq!("false", actual);
    }

    #[test]
    fn map_formatted_value_should_return_formatted_date_when_zero_date() {
        let value = AnyValue::Date(0);

        let actual = map_formatted_value(value);

        assert_eq!("1970-01-01", actual);
    }

    #[test]
    fn map_formatted_value_should_return_formatted_date_when_datetime_without_timezone() {
        let value = AnyValue::Datetime(1666469363000, TimeUnit::Milliseconds, &None);

        let actual = map_formatted_value(value);

        assert_eq!("2022-10-22 20:09:23", actual);
    }

    #[test]
    fn map_formatted_value_should_return_formatted_date_when_datetime_with_timezone() {
        let timezone = Some(String::from("America/New_York"));
        let value = AnyValue::Datetime(1666469363000, TimeUnit::Milliseconds, &timezone);

        let actual = map_formatted_value(value);

        assert_eq!("2022-10-22 20:09:23 EDT", actual);
    }

    #[test]
    fn map_formatted_value_should_return_formatted_interval_when_milliseconds() {
        let value = AnyValue::Duration(20200, TimeUnit::Milliseconds);

        let actual = map_formatted_value(value);

        assert_eq!("20200 milisecond", actual);
    }

    #[test]
    fn map_formatted_value_should_return_formatted_interval_when_microseconds() {
        let value = AnyValue::Duration(56, TimeUnit::Microseconds);

        let actual = map_formatted_value(value);

        assert_eq!("56 microsecond", actual);
    }

    #[test]
    fn map_formatted_value_should_return_formatted_interval_when_nanoseconds() {
        let value = AnyValue::Duration(9865, TimeUnit::Nanoseconds);

        let actual = map_formatted_value(value);

        assert_eq!("9.87 microsecond", actual);
    }

    #[test]
    fn escape_csv_string_should_return_self_when_no_special_chars_present() {
        let string = String::from("This is a test");

        let actual = escape_csv_string(string.to_owned());

        assert_eq!(string, actual);
    }

    #[test]
    fn escape_csv_string_should_return_qualified_value_when_comma_present() {
        let string = String::from("This is a, test");
        let expected = format!("\"{}\"", string);

        let actual = escape_csv_string(string);

        assert_eq!(expected, actual);
    }

    #[test]
    fn escape_csv_string_should_return_qualified_value_when_quote_present() {
        let string = String::from("This is \"a\" test");
        let expected = format!("\"{}\"", string.replace("\"", "\"\""));

        let actual = escape_csv_string(string);

        assert_eq!(expected, actual);
    }

    #[test]
    fn escape_csv_string_should_return_qualified_value_when_carriage_return_present() {
        let string = String::from("This is a\r test");
        let expected = format!("\"{}\"", string);

        let actual = escape_csv_string(string);

        assert_eq!(expected, actual);
    }

    #[test]
    fn escape_csv_string_should_return_qualified_value_when_new_line_present() {
        let string = String::from("This is a\n test");
        let expected = format!("\"{}\"", string);

        let actual = escape_csv_string(string.to_owned());

        assert_eq!(expected, actual);
    }
}
