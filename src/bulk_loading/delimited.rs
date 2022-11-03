use super::{
    loader::{CopyPipe, CopyResult},
    options::DataFileOptions,
};
use std::path::PathBuf;
use tokio::fs::File as TkFile;

pub struct DelimitedDataOptions {
    file_path: PathBuf,
    delimiter: char,
    qualified: bool,
}

impl DelimitedDataOptions {
    pub fn new(file_path: PathBuf, delimiter: char, qualified: bool) -> Self {
        Self {
            file_path,
            delimiter,
            qualified,
        }
    }
}

impl DataFileOptions for DelimitedDataOptions {
    #[inline]
    fn delimiter(&self) -> &char {
        &self.delimiter
    }

    #[inline]
    fn header(&self) -> &bool {
        &true
    }

    #[inline]
    fn qualified(&self) -> &bool {
        &self.qualified
    }
}

pub async fn load_delimited_data(
    copy: &mut CopyPipe,
    options: &DelimitedDataOptions,
) -> CopyResult {
    let file = TkFile::open(&options.file_path).await?;
    copy.read_from(file).await?;
    Ok(())
}
