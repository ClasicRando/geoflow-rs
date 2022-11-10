use super::{
    analyze::{Schema, SchemaParser},
    error::BulkDataResult,
    load::DataParser,
    options::DataFileOptions,
    utilities::{schema_from_dataframe, spool_dataframe_records},
};
use polars::prelude::{DataFrame, ParquetReader, SerReader};
use std::{fs::File, path::PathBuf};
use tokio::sync::mpsc::{error::SendError, Sender};

pub struct ParquetFileOptions {
    file_path: PathBuf,
}

impl ParquetFileOptions {
    pub fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }

    pub fn dataframe(&self) -> BulkDataResult<DataFrame> {
        let file = File::open(&self.file_path)?;
        Ok(ParquetReader::new(file).finish()?)
    }
}

impl DataFileOptions for ParquetFileOptions {}

pub struct ParquetSchemaParser(ParquetFileOptions);

impl SchemaParser for ParquetSchemaParser {
    type Options = ParquetFileOptions;

    fn new(options: ParquetFileOptions) -> Self
    where
        Self: Sized,
    {
        Self(options)
    }

    fn schema(&self) -> BulkDataResult<Schema> {
        let Some(table_name) = self.0.file_path.file_name().and_then(|f| f.to_str()) else {
            return Err(format!("Could not get filename for \"{:?}\"", &self.0.file_path).into())
        };
        let df = self.0.dataframe()?;
        schema_from_dataframe(table_name.to_owned(), df)
    }
}

pub struct ParquetFileParser(ParquetFileOptions);

impl ParquetFileParser {
    pub fn new(options: ParquetFileOptions) -> Self {
        Self(options)
    }
}

#[async_trait::async_trait]
impl DataParser for ParquetFileParser {
    type Options = ParquetFileOptions;

    fn options(&self) -> &Self::Options {
        &self.0
    }

    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let options = self.0;
        let df = match options.dataframe() {
            Ok(df) => df,
            Err(error) => return record_channel.send(Err(error)).await.err(),
        };
        spool_dataframe_records(df, record_channel).await
    }
}
