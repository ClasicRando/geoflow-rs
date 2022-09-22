use polars::prelude::{PolarsError, SerReader, SerWriter, DataFrame};
use polars::io::csv::CsvWriter;
use polars::io::ipc::IpcReader;
use sqlx::PgPool;
use std::fs::File;
use std::path::PathBuf;
use tokio::fs::File as TkFile;
use tempfile::NamedTempFile;

pub enum LoaderError {
    Polars(PolarsError),
    SQL(sqlx::Error),
    IO(std::io::Error),
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

impl From<std::io::Error> for LoaderError {
    fn from(error: std::io::Error) -> Self {
        Self::IO(error)
    }
}

pub struct CopyOptions {
    table_name: String,
    columns: Vec<String>,
}

impl CopyOptions {
    fn copy_statement(&self, delimiter: &u8, header: &bool, qualified: &bool) -> String {
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
}

fn create_temp_csv_data(mut dataframe: DataFrame) -> Result<PathBuf, LoaderError> {
    let csv_file = NamedTempFile::new()?;
    let path = csv_file.path().to_path_buf();
    CsvWriter::new(csv_file)
        .has_header(false)
        .with_delimiter(b',')
        .finish(&mut dataframe)?;
    Ok(path)
}

pub type BulkLoadResult = Result<u64, LoaderError>;

async fn load_delimited_data(
    copy_options: &CopyOptions,
    pool: &PgPool,
    file_path: &PathBuf,
    delimiter: &u8,
    qualified: &bool,
) -> BulkLoadResult {
    let copy_statement = copy_options.copy_statement(delimiter, &true, qualified);
    let mut copy = pool.copy_in_raw(copy_statement.as_str()).await?;
    let file = TkFile::open(file_path.as_path()).await?;
    copy.read_from(file).await?;
    Ok(copy.finish().await?)
}

async fn load_ipc_data(
    copy_options: &CopyOptions,
    pool: &PgPool,
    file_path: &PathBuf,
) -> BulkLoadResult {
    let file = File::open(file_path.as_path())?;
    let df = IpcReader::new(file).finish()?;
    let path = create_temp_csv_data(df)?;
    load_delimited_data(copy_options, pool, &path, &b',', &true).await
}

enum BulkDataLoader {
    DelimitedData {
        file_path: PathBuf,
        delimiter: u8,
        qualified: bool,
    },
    Mdb,
    Excel,
    Dbf,
    Shape,
    Ipc {
        file_path: PathBuf,
    },
}

impl BulkDataLoader {
    async fn load_data(&self, copy_options: CopyOptions, pool: PgPool) -> BulkLoadResult {
        match self {
            Self::DelimitedData {
                file_path,
                delimiter,
                qualified,
            } => {
                load_delimited_data(&copy_options, &pool, file_path, delimiter, qualified).await?;
            }
            Self::Mdb => todo!(),
            Self::Excel => todo!(),
            Self::Dbf => todo!(),
            Self::Shape => todo!(),
            Self::Ipc { file_path } => {
                load_ipc_data(&copy_options, &pool, file_path).await?;
            }
        }
        Ok(2)
    }
}
