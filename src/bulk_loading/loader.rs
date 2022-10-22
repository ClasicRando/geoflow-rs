use calamine::{open_workbook_auto, DataType, Reader};
use geo_types::Geometry;
use geojson::FeatureReader;
use itertools::Itertools;
use polars::io::ipc::IpcReader;
use polars::prelude::{AnyValue, DataFrame, ParquetReader, SerReader, TimeUnit};
use shapefile::dbase::FieldValue;
use shapefile::Shape;
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgCopyIn;
use sqlx::{PgPool, Postgres};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use tokio::fs::File as TkFile;
use wkt::ToWkt;

use super::{
    error::BulkDataError,
    options::{DataFileOptions, DefaultFileOptions, DelimitedDataOptions, ExcelOptions},
};

pub struct CopyOptions {
    table_name: String,
    columns: Vec<String>,
}

impl CopyOptions {
    pub fn new(table_name: &str, columns: &Vec<&str>) -> Self {
        Self {
            table_name: table_name.to_owned(),
            columns: columns.iter().map(|s| s.to_string()).collect_vec(),
        }
    }

    fn copy_statement<O: DataFileOptions>(&self, options: &O) -> String {
        format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER '{}', HEADER {}, NULL ''{})",
            self.table_name.to_lowercase(),
            self.columns.join(","),
            options.delimiter(),
            if *options.header() { "true" } else { "false" },
            if *options.qualified() {
                ", QUOTE '\"', ESCAPE '\"'"
            } else {
                ""
            }
        )
    }
}

pub type CopyPipe = PgCopyIn<PoolConnection<Postgres>>;
pub type BulkLoadResult = Result<u64, BulkDataError>;
pub type CopyResult = Result<(), BulkDataError>;

async fn copy_csv_iter<I: Iterator<Item = String>>(copy: &mut CopyPipe, csv_iter: I) -> CopyResult {
    let mut csv_data = csv_iter.map(|s| escape_csv_string(s)).join(",");
    csv_data.push('\n');
    copy.send(csv_data.as_bytes()).await?;
    Ok(())
}

async fn copy_csv_values<I: IntoIterator<Item = String>>(
    copy: &mut CopyPipe,
    csv_values: I,
) -> CopyResult {
    let mut csv_data = csv_values
        .into_iter()
        .map(|s| escape_csv_string(s))
        .join(",");
    csv_data.push('\n');
    copy.send(csv_data.as_bytes()).await?;
    Ok(())
}

fn escape_csv_string(csv_string: String) -> String {
    if csv_string
        .chars()
        .any(|c| c == '"' || c == ',' || c == '\n' || c == '\r')
    {
        format!("\"{}\"", csv_string.replace("\"", "\"\""))
    } else {
        csv_string
    }
}

fn map_formatted_value(value: AnyValue) -> String {
    match value {
        AnyValue::Null => String::new(),
        AnyValue::Utf8(string) => string.to_owned(),
        AnyValue::Utf8Owned(string) => string,
        AnyValue::Duration(duration, unit) => {
            match unit {
                TimeUnit::Microseconds => format!("{} microsecond", duration),
                TimeUnit::Milliseconds => format!("{} milisecond", duration),
                TimeUnit::Nanoseconds => format!("{:.2} microsecond", duration as f64 / 1000.0_f64),
            }
        }
        _ => format!("{}", value),
    }
}

async fn load_dataframe(copy: &mut CopyPipe, dataframe: DataFrame) -> CopyResult {
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

async fn load_delimited_data(copy: &mut CopyPipe, file_path: &Path) -> CopyResult {
    let file = TkFile::open(file_path).await?;
    copy.read_from(file).await?;
    Ok(())
}

pub fn map_excel_value(value: &DataType) -> Result<String, BulkDataError> {
    Ok(match value {
        DataType::String(s) => s.replace("_x000d_", "\n").replace("_x000a_", "\r"),
        DataType::DateTime(_) => {
            let formatted_datetime = value
                .as_datetime()
                .ok_or(format!(
                    "Cell error. Should be datetime but found something else. {}",
                    value
                ))?
                .format("%Y-%m-%d %H:%M:%S");
            format!("{}", formatted_datetime)
        }
        DataType::Error(e) => return Err(BulkDataError::Generic(format!("Cell error, {}", e))),
        DataType::Empty => String::new(),
        _ => format!("{}", value),
    })
}

async fn load_excel_data(copy: &mut CopyPipe, options: &ExcelOptions<'_>) -> CopyResult {
    let (file_path, sheet_name) = (options.file_path(), options.sheet_name());
    let mut workbook = open_workbook_auto(file_path)?;
    let sheet = match workbook.worksheet_range(sheet_name) {
        Some(Ok(sheet)) => sheet,
        _ => {
            return Err(BulkDataError::Generic(format!(
                "Could not find sheet \"{}\" in {:?}",
                sheet_name, file_path
            )))
        }
    };
    let mut rows = sheet.rows();
    let header = match rows.next() {
        Some(row) => row,
        None => {
            return Err(BulkDataError::Generic(format!(
                "Could not find a header row for excel file {:?}",
                file_path
            )))
        }
    };
    let header_size = header.len();
    for (row_num, row) in rows.enumerate() {
        let row_data = row
            .iter()
            .map(|value| map_excel_value(value))
            .collect::<Result<Vec<String>, _>>()?;
        if row_data.len() != header_size {
            return Err(BulkDataError::Generic(format!(
                "Excel row {} has {} values but expected {}",
                row_num + 1,
                row_data.len(),
                header_size
            )));
        }
        copy_csv_values(copy, row_data).await?;
    }
    Ok(())
}

fn map_field_value(value: FieldValue) -> String {
    match value {
        FieldValue::Character(str) => str.unwrap_or_default(),
        FieldValue::Numeric(n) => n.map(|f| f.to_string()).unwrap_or_default(),
        FieldValue::Logical(l) => l.map(|b| b.to_string()).unwrap_or_default(),
        FieldValue::Date(date) => date
            .map(|d| format!("{}-{}-{}", d.year(), d.month(), d.day()))
            .unwrap_or_default(),
        FieldValue::Float(f) => f.map(|f| f.to_string()).unwrap_or("".into()),
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Currency(c) => c.to_string(),
        FieldValue::DateTime(dt) => {
            let date = dt.date();
            let time = dt.time();
            format!(
                "{}-{}-{} {}:{}:{}",
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

async fn load_shape_data(copy: &mut CopyPipe, file_path: &Path) -> CopyResult {
    let mut reader = shapefile::Reader::from_path(file_path)?;
    for feature in reader.iter_shapes_and_records() {
        let (shape, record) = feature?;
        let wkt = match shape {
            Shape::NullShape => String::new(),
            _ => {
                let geo = geo_types::Geometry::<f64>::try_from(shape)?;
                format!("{}", geo.wkt_string())
            }
        };
        let csv_row = record
            .into_iter()
            .map(|(_, value)| map_field_value(value))
            .chain(std::iter::once(wkt));
        copy_csv_iter(copy, csv_row).await?;
    }
    Ok(())
}

async fn load_geo_json_data(copy: &mut CopyPipe, file_path: &Path) -> CopyResult {
    let file = File::open(file_path)?;
    let buff_reader = BufReader::new(file);
    let feature_reader = FeatureReader::from_reader(buff_reader);
    for feature in feature_reader.features() {
        let feature = feature?;
        let geom = feature
            .geometry
            .as_ref()
            .and_then(|g| Geometry::<f64>::try_from(g).ok())
            .map(|g| g.wkt_string())
            .unwrap_or_default();
        let csv_row = feature
            .properties_iter()
            .map(|(_, value)| format!("{}", value))
            .chain(std::iter::once(geom));
        copy_csv_iter(copy, csv_row).await?;
    }
    Ok(())
}

async fn load_parquet_data(copy: &mut CopyPipe, file_path: &Path) -> CopyResult {
    let file = File::open(file_path)?;
    let df = ParquetReader::new(file).finish()?;
    load_dataframe(copy, df).await
}

async fn load_ipc_data(copy: &mut CopyPipe, file_path: &Path) -> CopyResult {
    let file = File::open(file_path)?;
    let df = IpcReader::new(file).finish()?;
    load_dataframe(copy, df).await
}

pub enum BulkDataLoader<'p> {
    DelimitedData { options: DelimitedDataOptions<'p> },
    Excel { options: ExcelOptions<'p> },
    Shape { options: DefaultFileOptions<'p> },
    GeoJSON { options: DefaultFileOptions<'p> },
    Parquet { options: DefaultFileOptions<'p> },
    Ipc { options: DefaultFileOptions<'p> },
}

impl<'p> BulkDataLoader<'p> {
    pub async fn load_data(&self, copy_options: CopyOptions, pool: PgPool) -> BulkLoadResult {
        let copy_statement = match self {
            BulkDataLoader::DelimitedData { options } => copy_options.copy_statement(options),
            BulkDataLoader::Excel { options } => copy_options.copy_statement(options),
            BulkDataLoader::Shape { options } => copy_options.copy_statement(options),
            BulkDataLoader::GeoJSON { options } => copy_options.copy_statement(options),
            BulkDataLoader::Parquet { options } => copy_options.copy_statement(options),
            BulkDataLoader::Ipc { options } => copy_options.copy_statement(options),
        };
        let mut copy = pool.copy_in_raw(copy_statement.as_str()).await?;
        let result = match self {
            Self::DelimitedData { options } => {
                load_delimited_data(&mut copy, options.file_path()).await
            }
            Self::Excel { options } => load_excel_data(&mut copy, options).await,
            Self::Shape { options } => load_shape_data(&mut copy, options.file_path()).await,
            Self::GeoJSON { options } => load_geo_json_data(&mut copy, options.file_path()).await,
            Self::Parquet { options } => load_parquet_data(&mut copy, options.file_path()).await,
            Self::Ipc { options } => load_ipc_data(&mut copy, options.file_path()).await,
        };
        match result {
            Ok(_) => Ok(copy.finish().await?),
            Err(error) => {
                copy.abort(format!("{}", error)).await?;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use polars::prelude::{TimeUnit};

    use super::*;

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
}
