use std::{fs::File, path::Path};

use polars::prelude::{IpcReader, SerReader};

use super::{
    loader::{CopyPipe, CopyResult},
    options::DefaultFileOptions,
    utilities::load_dataframe,
};

pub async fn load_ipc_data(copy: &mut CopyPipe, options: &DefaultFileOptions) -> CopyResult {
    let file = File::open(&options.file_path)?;
    let df = IpcReader::new(file).finish()?;
    load_dataframe(copy, df).await
}
