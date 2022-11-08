use polars::prelude::{DataFrame, ParquetReader, SerReader};
use std::{fs::File, path::Path};

use super::error::BulkDataResult;

pub fn parquet_data_to_df(file_path: &Path) -> BulkDataResult<DataFrame> {
    let file = File::open(file_path)?;
    Ok(ParquetReader::new(file).finish()?)
}
