use super::{
    analyze::Schema,
    error::BulkDataResult,
    load::{RecordSpoolChannel, RecordSpoolResult},
    options::DataOptions,
    utilities::{schema_from_dataframe, spool_dataframe_records},
};
use polars::prelude::{DataFrame, IpcReader, SerReader};
use serde::{Deserialize, Serialize};
use std::{fs::File, path::PathBuf};

#[derive(Deserialize, Serialize)]
pub struct IpcFileOptions {
    file_path: PathBuf,
}

impl IpcFileOptions {
    pub fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }

    pub fn dataframe(&self) -> BulkDataResult<DataFrame> {
        let file = File::open(&self.file_path)?;
        Ok(IpcReader::new(file).finish()?)
    }
}

impl DataOptions for IpcFileOptions {}

pub fn schema(options: &IpcFileOptions) -> BulkDataResult<Schema> {
    let Some(table_name) = options.file_path.file_name().and_then(|f| f.to_str()) else {
        return Err(format!("Could not get filename for \"{:?}\"", &options.file_path).into())
    };
    let df = options.dataframe()?;
    schema_from_dataframe(table_name.to_owned(), df)
}

pub async fn spool_records(options: &IpcFileOptions, record_channel: &mut RecordSpoolChannel) -> RecordSpoolResult {
    let df = match options.dataframe() {
        Ok(df) => df,
        Err(error) => return record_channel.send(Err(error)).await.err(),
    };
    spool_dataframe_records(df, record_channel).await
}
