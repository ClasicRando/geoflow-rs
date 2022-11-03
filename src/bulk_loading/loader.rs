use std::path::PathBuf;

use super::{
    delimited::{load_delimited_data, DelimitedDataOptions},
    error::BulkDataError,
    excel::{load_excel_data, ExcelOptions},
    geo_json::load_geo_json_data,
    ipc::load_ipc_data,
    options::{DataFileOptions, DefaultFileOptions},
    parquet::load_parquet_data,
    shape::load_shape_data,
    utilities::escape_csv_string,
};
use itertools::Itertools;
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgCopyIn;
use sqlx::{PgPool, Postgres};

pub type CopyPipe = PgCopyIn<PoolConnection<Postgres>>;
pub type BulkLoadResult = Result<u64, BulkDataError>;
pub type CopyResult = Result<(), BulkDataError>;

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

pub async fn copy_csv_iter<I: Iterator<Item = String>>(
    copy: &mut CopyPipe,
    csv_iter: I,
) -> CopyResult {
    let mut csv_data = csv_iter.map(|s| escape_csv_string(s)).join(",");
    csv_data.push('\n');
    copy.send(csv_data.as_bytes()).await?;
    Ok(())
}

pub async fn copy_csv_values<I: IntoIterator<Item = String>>(
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

pub enum BulkDataLoader {
    DelimitedData { options: DelimitedDataOptions },
    Excel { options: ExcelOptions },
    Shape { options: DefaultFileOptions },
    GeoJSON { options: DefaultFileOptions },
    Parquet { options: DefaultFileOptions },
    Ipc { options: DefaultFileOptions },
}

impl BulkDataLoader {
    pub fn from_delimited_data(file_path: PathBuf, delimiter: char, qualified: bool) -> Self {
        Self::DelimitedData {
            options: DelimitedDataOptions::new(file_path, delimiter, qualified),
        }
    }

    pub fn from_excel_data(file_path: PathBuf, sheet_name: String) -> Self {
        Self::Excel {
            options: ExcelOptions::new(file_path, sheet_name),
        }
    }
    
    pub fn from_shape_data(file_path: PathBuf) -> Self {
        Self::Shape {
            options: DefaultFileOptions::new(file_path),
        }
    }
    
    pub fn from_geo_json_Data(file_path: PathBuf) -> Self {
        Self::GeoJSON {
            options: DefaultFileOptions::new(file_path),
        }
    }
    
    pub fn from_parquet_data(file_path: PathBuf) -> Self {
        Self::Parquet {
            options: DefaultFileOptions::new(file_path),
        }
    }
    
    pub fn from_ipc_data(file_path: PathBuf) -> Self {
        Self::Ipc {
            options: DefaultFileOptions::new(file_path),
        }
    }

    pub async fn load_data(&self, copy_options: CopyOptions, pool: PgPool) -> BulkLoadResult {
        let copy_statement = match self {
            BulkDataLoader::DelimitedData { options } => copy_options.copy_statement(options),
            BulkDataLoader::Excel { options } => copy_options.copy_statement(options),
            BulkDataLoader::Shape { options } => copy_options.copy_statement(options),
            BulkDataLoader::GeoJSON { options } => copy_options.copy_statement(options),
            BulkDataLoader::Parquet { options } => copy_options.copy_statement(options),
            BulkDataLoader::Ipc { options } => copy_options.copy_statement(options),
        };
        let mut copy = pool.copy_in_raw(&copy_statement).await?;
        let result = match self {
            Self::DelimitedData { options } => load_delimited_data(&mut copy, options).await,
            Self::Excel { options } => load_excel_data(&mut copy, options).await,
            Self::Shape { options } => load_shape_data(&mut copy, options).await,
            Self::GeoJSON { options } => load_geo_json_data(&mut copy, options).await,
            Self::Parquet { options } => load_parquet_data(&mut copy, options).await,
            Self::Ipc { options } => load_ipc_data(&mut copy, options).await,
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
