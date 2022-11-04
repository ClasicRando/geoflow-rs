use std::fs::File;

use polars::prelude::{ParquetReader, SerReader};

use super::{
    loader::{CopyPipe, CopyResult},
    options::DefaultFileOptions,
    utilities::load_dataframe,
};

pub async fn load_parquet_data(copy: &mut CopyPipe, options: &DefaultFileOptions) -> CopyResult {
    let file = File::open(&options.file_path)?;
    let df = ParquetReader::new(file).finish()?;
    load_dataframe(copy, df).await
}
