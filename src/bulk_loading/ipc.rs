use std::{fs::File, path::Path};
use polars::prelude::{IpcReader, SerReader, DataFrame};

use super::error::BulkDataResult;

pub fn ipc_data_to_df(file_path: &Path) -> BulkDataResult<DataFrame> {
    let file = File::open(file_path)?;
    Ok(IpcReader::new(file).finish()?)
}
