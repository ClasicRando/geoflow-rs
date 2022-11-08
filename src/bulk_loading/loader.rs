use std::path::PathBuf;

use super::{
    delimited::{DelimitedDataOptions, DelimitedDataParser},
    error::{BulkDataError, BulkDataResult},
    excel::{ExcelDataParser, ExcelOptions},
    geo_json::GeoJsonParser,
    ipc::ipc_data_to_df,
    options::{DataFileOptions, DefaultFileOptions},
    parquet::parquet_data_to_df,
    shape::ShapeDataParser,
    utilities::{escape_csv_string, DataFrameParser},
};
use itertools::Itertools;
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgCopyIn;
use sqlx::{PgPool, Postgres};
use tokio::sync::mpsc::{channel as mpsc_channel, error::SendError, Sender};

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

pub fn csv_iter_to_string<I: Iterator<Item = String>>(csv_iter: I) -> String {
    let mut csv_data = csv_iter.map(|s| escape_csv_string(s)).join(",");
    csv_data.push('\n');
    csv_data
}

pub fn csv_values_to_string<I: IntoIterator<Item = String>>(csv_values: I) -> String {
    let mut csv_data = csv_values
        .into_iter()
        .map(|s| escape_csv_string(s))
        .join(",");
    csv_data.push('\n');
    csv_data
}

#[async_trait::async_trait]
pub trait DataParser {
    type Options: DataFileOptions;

    fn new(options: Self::Options) -> BulkDataResult<Self>
    where
        Self: Sized;
    fn options(&self) -> &Self::Options;
    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>>;
}

pub struct DataLoader<P: DataParser + Send + Sync + 'static>(P);

impl DataLoader<DelimitedDataParser> {
    pub fn from_delimited(
        file_path: PathBuf,
        delimiter: char,
        qualified: bool,
    ) -> BulkDataResult<Self> {
        let options = DelimitedDataOptions::new(file_path, delimiter, qualified);
        Ok(Self::new(DelimitedDataParser::new(options)?))
    }
}

impl DataLoader<ExcelDataParser> {
    pub fn from_excel(file_path: PathBuf, sheet_name: String) -> BulkDataResult<Self> {
        let options = ExcelOptions::new(file_path, sheet_name);
        Ok(Self::new(ExcelDataParser::new(options)?))
    }
}

impl DataLoader<ShapeDataParser> {
    pub fn from_shape(file_path: PathBuf) -> BulkDataResult<Self> {
        let options = DefaultFileOptions::new(file_path);
        Ok(Self::new(ShapeDataParser::new(options)?))
    }
}

impl DataLoader<GeoJsonParser> {
    pub fn from_geo_json(file_path: PathBuf) -> BulkDataResult<Self> {
        let options = DefaultFileOptions::new(file_path);
        Ok(Self::new(GeoJsonParser::new(options)?))
    }
}

impl DataLoader<DataFrameParser> {
    pub fn from_parquet(file_path: PathBuf) -> BulkDataResult<Self> {
        let dataframe = parquet_data_to_df(&file_path)?;
        let options = DefaultFileOptions::new(file_path);
        let mut parser = DataFrameParser::new(options)?;
        parser.set_dataframe(dataframe);
        Ok(Self::new(parser))
    }

    pub fn from_ipc(file_path: PathBuf) -> BulkDataResult<Self> {
        let dataframe = ipc_data_to_df(&file_path)?;
        let options = DefaultFileOptions::new(file_path);
        let mut parser = DataFrameParser::new(options)?;
        parser.set_dataframe(dataframe);
        Ok(Self::new(parser))
    }
}

impl<P: DataParser + Send + Sync + 'static> DataLoader<P> {
    fn new(parser: P) -> Self {
        Self(parser)
    }

    pub async fn load_data(self, copy_options: CopyOptions, pool: PgPool) -> BulkLoadResult {
        let copy_statement = copy_options.copy_statement(self.0.options());
        let mut copy = pool.copy_in_raw(&copy_statement).await?;
        let (mut tx, mut rx) = mpsc_channel(1000);
        let spool_handle = tokio::spawn(async move {
            let error = self.0.spool_records(&mut tx).await;
            drop(tx);
            error
        });
        let result = loop {
            match rx.recv().await {
                Some(msg) => match msg {
                    Ok(record) => {
                        if let Err(error) = copy.send(record.as_bytes()).await {
                            break Err(error.into());
                        }
                    }
                    Err(error) => break Err(error),
                },
                None => break Ok(()),
            }
        };
        rx.close();
        match spool_handle.await {
            Ok(Some(value)) => println!("SendError\n{:?}", value.0),
            Ok(None) => println!("Finished spool handle successfully"),
            Err(error) => println!("Error trying to finish the spool handle\n{}", error),
        }
        match result {
            Ok(_) => Ok(copy.finish().await?),
            Err(error) => {
                copy.abort(format!("{}", error)).await?;
                Err(error)
            }
        }
    }
}

// pub enum BulkDataLoader {
//     DelimitedData { options: DelimitedDataOptions },
//     Excel { options: ExcelOptions },
//     Shape { options: DefaultFileOptions },
//     GeoJSON { options: DefaultFileOptions },
//     Parquet { options: DefaultFileOptions },
//     Ipc { options: DefaultFileOptions },
// }

// impl BulkDataLoader {
//     pub fn from_delimited_data(file_path: PathBuf, delimiter: char, qualified: bool) -> Self {
//         Self::DelimitedData {
//             options: DelimitedDataOptions::new(file_path, delimiter, qualified),
//         }
//     }

//     pub fn from_excel_data(file_path: PathBuf, sheet_name: String) -> Self {
//         Self::Excel {
//             options: ExcelOptions::new(file_path, sheet_name),
//         }
//     }

//     pub fn from_shape_data(file_path: PathBuf) -> Self {
//         Self::Shape {
//             options: DefaultFileOptions::new(file_path),
//         }
//     }

//     pub fn from_geo_json_data(file_path: PathBuf) -> Self {
//         Self::GeoJSON {
//             options: DefaultFileOptions::new(file_path),
//         }
//     }

//     pub fn from_parquet_data(file_path: PathBuf) -> Self {
//         Self::Parquet {
//             options: DefaultFileOptions::new(file_path),
//         }
//     }

//     pub fn from_ipc_data(file_path: PathBuf) -> Self {
//         Self::Ipc {
//             options: DefaultFileOptions::new(file_path),
//         }
//     }

//     pub async fn load_data(&self, copy_options: CopyOptions, pool: PgPool) -> BulkLoadResult {
//         let copy_statement = match self {
//             BulkDataLoader::DelimitedData { options } => copy_options.copy_statement(options),
//             BulkDataLoader::Excel { options } => copy_options.copy_statement(options),
//             BulkDataLoader::Shape { options } => copy_options.copy_statement(options),
//             BulkDataLoader::GeoJSON { options } => copy_options.copy_statement(options),
//             BulkDataLoader::Parquet { options } => copy_options.copy_statement(options),
//             BulkDataLoader::Ipc { options } => copy_options.copy_statement(options),
//         };
//         let mut copy = pool.copy_in_raw(&copy_statement).await?;
//         let result = match self {
//             Self::DelimitedData { options } => load_delimited_data(&mut copy, options).await,
//             Self::Excel { options } => load_excel_data(&mut copy, options).await,
//             Self::Shape { options } => load_shape_data(&mut copy, options).await,
//             Self::GeoJSON { options } => load_geo_json_data(&mut copy, options).await,
//             Self::Parquet { options } => load_parquet_data(&mut copy, options).await,
//             Self::Ipc { options } => load_ipc_data(&mut copy, options).await,
//         };
//         match result {
//             Ok(_) => Ok(copy.finish().await?),
//             Err(error) => {
//                 copy.abort(format!("{}", error)).await?;
//                 Err(error)
//             }
//         }
//     }
// }
