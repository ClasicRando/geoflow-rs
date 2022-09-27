use calamine::{open_workbook_auto, DataType, Reader};
use geo_types::Geometry;
use geojson::FeatureReader;
use itertools::Itertools;
use polars::io::ipc::IpcReader;
use polars::prelude::{AnyValue, DataFrame, ParquetReader, PolarsError, SerReader};
use shapefile::dbase::FieldValue;
use shapefile::Shape;
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgCopyIn;
use sqlx::{PgPool, Postgres};
use std::fmt::{Display, Write};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use tokio::fs::File as TkFile;
use wkt::ToWkt;

#[derive(Debug)]
pub enum LoaderError {
    Generic(String),
    Polars(PolarsError),
    SQL(sqlx::Error),
    Fmt(std::fmt::Error),
    IO(std::io::Error),
    Excel(calamine::Error),
    Shp(shapefile::Error),
    GeoJSON(geojson::Error),
}

impl std::error::Error for LoaderError {}

impl Display for LoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoaderError::Generic(string) => write!(f, "Loader Error\n{}", string),
            LoaderError::Polars(error) => write!(f, "Polars Error\n{}", error),
            LoaderError::SQL(error) => write!(f, "Polars Error\n{}", error),
            LoaderError::Fmt(error) => write!(f, "Format Error\n{}", error),
            LoaderError::IO(error) => write!(f, "IO Error\n{}", error),
            LoaderError::Excel(error) => write!(f, "Excel Error\n{}", error),
            LoaderError::Shp(error) => write!(f, "Shapefile Error\n{}", error),
            LoaderError::GeoJSON(error) => write!(f, "GeoJSON Error\n{}", error),
        }
    }
}

impl From<PolarsError> for LoaderError {
    fn from(error: PolarsError) -> Self {
        Self::Polars(error)
    }
}

impl From<sqlx::Error> for LoaderError {
    fn from(error: sqlx::Error) -> Self {
        Self::SQL(error)
    }
}

impl From<std::fmt::Error> for LoaderError {
    fn from(error: std::fmt::Error) -> Self {
        Self::Fmt(error)
    }
}

impl From<std::io::Error> for LoaderError {
    fn from(error: std::io::Error) -> Self {
        Self::IO(error)
    }
}

impl From<calamine::Error> for LoaderError {
    fn from(error: calamine::Error) -> Self {
        Self::Excel(error)
    }
}

impl From<&str> for LoaderError {
    fn from(error: &str) -> Self {
        Self::Generic(error.to_owned())
    }
}

impl From<String> for LoaderError {
    fn from(error: String) -> Self {
        Self::Generic(error)
    }
}

impl From<shapefile::Error> for LoaderError {
    fn from(error: shapefile::Error) -> Self {
        Self::Shp(error)
    }
}

impl From<geojson::Error> for LoaderError {
    fn from(error: geojson::Error) -> Self {
        Self::GeoJSON(error)
    }
}

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

    fn copy_statement(&self, delimiter: &char, header: &bool, qualified: &bool) -> String {
        format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER '{}', HEADER {}, NULL ''{})",
            self.table_name.to_lowercase(),
            self.columns.join(","),
            delimiter,
            if *header { "true" } else { "false" },
            if *qualified {
                ", QUOTE '\"', ESCAPE '\"'"
            } else {
                ""
            }
        )
    }

    fn csv_copy_statement(&self) -> String {
        format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER false, NULL '', QUOTE '\"', ESCAPE '\"')",
            self.table_name.to_lowercase(),
            self.columns.join(","),
        )
    }
}

pub type CopyPipe = PgCopyIn<PoolConnection<Postgres>>;
pub type BulkLoadResult = Result<u64, LoaderError>;
pub type CopyResult = Result<(), LoaderError>;

async fn copy_csv_row(copy: &mut CopyPipe, mut csv_data: String) -> Result<(), LoaderError> {
    if csv_data.ends_with(',') {
        csv_data.pop();
    }
    csv_data.push('\n');
    copy.send(csv_data.as_bytes()).await?;
    Ok(())
}

fn escape_csv_string_into_buffer(buffer: &mut String, csv_string: &str) -> std::fmt::Result {
    if csv_string.contains('"')
        || csv_string.contains(',')
        || csv_string.contains('\n')
        || csv_string.contains('\r')
    {
        write!(buffer, "\"{}\",", csv_string.replace("\"", "\"\""))
    } else {
        write!(buffer, "{},", csv_string)
    }
}

fn escape_option_csv_string_into_buffer(
    buffer: &mut String,
    option_csv_string: &Option<String>,
) -> std::fmt::Result {
    match option_csv_string {
        None => write!(buffer, ","),
        Some(str) => escape_csv_string_into_buffer(buffer, str),
    }
}

fn append_formatted_value(buffer: &mut String, value: AnyValue) -> std::fmt::Result {
    match value {
        AnyValue::Null => write!(buffer, ","),
        AnyValue::Utf8(string) => escape_csv_string_into_buffer(buffer, string),
        AnyValue::Utf8Owned(string) => escape_csv_string_into_buffer(buffer, &string),
        _ => write!(buffer, "{},", value),
    }
}

async fn load_dataframe(copy: &mut CopyPipe, dataframe: DataFrame) -> CopyResult {
    let mut iters = dataframe.iter().map(|s| s.iter()).collect::<Vec<_>>();
    for _ in 0..dataframe.height() {
        let mut csv_row = String::new();
        for iter in &mut iters {
            let value = iter
                .next()
                .ok_or("Dataframe value was not found. This should never happen".to_string())?;
            append_formatted_value(&mut csv_row, value)?;
        }
        copy_csv_row(copy, csv_row).await?;
    }
    Ok(())
}

async fn load_delimited_data(copy: &mut CopyPipe, file_path: &Path) -> CopyResult {
    let file = TkFile::open(file_path).await?;
    copy.read_from(file).await?;
    Ok(())
}

fn append_excel_value(buffer: &mut String, value: &DataType) -> Result<(), LoaderError> {
    match value {
        DataType::String(s) => escape_csv_string_into_buffer(
            buffer,
            &s.replace("_x000d_", "\n").replace("_x000a_", "\r"),
        )?,
        DataType::DateTime(_) => {
            let formatted_datetime = value
                .as_datetime()
                .ok_or(format!(
                    "Cell error. Should be datetime but found something else. {}",
                    value
                ))?
                .format("%Y-%m-%d %H:%M:%S");
            write!(buffer, "{},", formatted_datetime)?;
        }
        DataType::Error(e) => return Err(LoaderError::Generic(format!("Cell error, {}", e))),
        DataType::Empty => write!(buffer, ",")?,
        _ => write!(buffer, "{},", value)?,
    }
    Ok(())
}

async fn load_excel_data(copy: &mut CopyPipe, file_path: &Path, sheet_name: &String) -> CopyResult {
    let mut workbook = open_workbook_auto(file_path)?;
    let sheet = match workbook.worksheet_range(&sheet_name) {
        Some(Ok(sheet)) => sheet,
        _ => {
            return Err(LoaderError::Generic(format!(
                "Could not find sheet \"{}\" in {:?}",
                sheet_name, file_path
            )))
        }
    };
    let mut rows = sheet.rows();
    let header = match rows.next() {
        Some(row) => row,
        None => {
            return Err(LoaderError::Generic(format!(
                "Could not find a header row for excel file {:?}",
                file_path
            )))
        }
    };
    let header_size = header.len();
    let mut row_num = 0;
    for row in rows {
        row_num += 1;
        let mut value_count = 0;
        let mut csv_row = String::new();
        for col in row {
            append_excel_value(&mut csv_row, col)?;
            value_count += 1;
        }
        if value_count != header_size {
            return Err(LoaderError::Generic(format!(
                "Excel row {} has {} values but expected {}",
                row_num, value_count, header_size
            )));
        }
        copy_csv_row(copy, csv_row).await?;
    }
    Ok(())
}

fn append_option_to_buffer<T: Display>(
    buffer: &mut String,
    option_value: &Option<T>,
) -> std::fmt::Result {
    match option_value {
        None => write!(buffer, ","),
        Some(v) => write!(buffer, "{},", v),
    }
}

fn append_field_value(buffer: &mut String, value: &FieldValue) -> std::fmt::Result {
    match value {
        FieldValue::Character(str) => escape_option_csv_string_into_buffer(buffer, str),
        FieldValue::Numeric(n) => append_option_to_buffer(buffer, n),
        FieldValue::Logical(l) => append_option_to_buffer(buffer, l),
        FieldValue::Date(date) => match date {
            Some(d) => write!(buffer, "{}-{}-{},", d.year(), d.month(), d.day()),
            None => write!(buffer, ","),
        },
        FieldValue::Float(f) => append_option_to_buffer(buffer, f),
        FieldValue::Integer(i) => write!(buffer, "{},", i),
        FieldValue::Currency(c) => write!(buffer, "{},", c),
        FieldValue::DateTime(dt) => {
            let date = dt.date();
            let time = dt.time();
            write!(
                buffer,
                "{}-{}-{} {}:{}:{},",
                date.year(),
                date.month(),
                date.day(),
                time.hours(),
                time.minutes(),
                time.seconds()
            )
        }
        FieldValue::Double(d) => write!(buffer, "{},", d),
        FieldValue::Memo(m) => escape_csv_string_into_buffer(buffer, &m),
    }
}

async fn load_shape_data(copy: &mut CopyPipe, file_path: &Path) -> CopyResult {
    let mut reader = shapefile::Reader::from_path(file_path)?;
    for feature in reader.iter_shapes_and_records() {
        let (shape, record) = feature?;
        let mut csv_row = String::new();
        for (_, value) in record {
            append_field_value(&mut csv_row, &value)?
        }
        match shape {
            Shape::NullShape => write!(&mut csv_row, ",")?,
            _ => {
                let geo = geo_types::Geometry::<f64>::try_from(shape)?;
                write!(&mut csv_row, "{},", geo.wkt_string())?;
            }
        }
        copy_csv_row(copy, csv_row).await?;
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
            .map(|(_, value)| value as &dyn Display)
            .chain(std::iter::once(&geom as &dyn Display))
            .join(",");
        copy_csv_row(copy, csv_row).await?;
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
    DelimitedData {
        file_path: &'p Path,
        delimiter: char,
        qualified: bool,
    },
    Excel {
        file_path: &'p Path,
        sheet_name: String,
    },
    Shape {
        file_path: &'p Path,
    },
    GeoJSON {
        file_path: &'p Path,
    },
    Parquet {
        file_path: &'p Path,
    },
    Ipc {
        file_path: &'p Path,
    },
}

impl<'p> BulkDataLoader<'p> {
    pub async fn load_data(&self, copy_options: CopyOptions, pool: PgPool) -> BulkLoadResult {
        let copy_statement = if let Self::DelimitedData {
            delimiter,
            qualified,
            ..
        } = self
        {
            copy_options.copy_statement(delimiter, &true, qualified)
        } else {
            copy_options.csv_copy_statement()
        };
        let mut copy = pool.copy_in_raw(copy_statement.as_str()).await?;
        let result = match self {
            Self::DelimitedData { file_path, .. } => {
                load_delimited_data(&mut copy, file_path).await
            }
            Self::Excel {
                file_path,
                sheet_name,
            } => load_excel_data(&mut copy, file_path, sheet_name).await,
            Self::Shape { file_path } => load_shape_data(&mut copy, file_path).await,
            Self::GeoJSON { file_path } => load_geo_json_data(&mut copy, file_path).await,
            Self::Parquet { file_path } => load_parquet_data(&mut copy, file_path).await,
            Self::Ipc { file_path } => load_ipc_data(&mut copy, file_path).await,
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
