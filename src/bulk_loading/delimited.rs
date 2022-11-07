use super::{
    error::BulkDataResult,
    loader::{CopyPipe, CopyResult, DataParser},
    options::DataFileOptions,
};
use std::path::PathBuf;
use tokio::{
    fs::File as TkFile,
    io::{AsyncBufReadExt, BufReader as TkBufReader},
    sync::mpsc::{error::SendError, Sender},
};

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

pub struct DelimitedDataParser {
    options: DelimitedDataOptions,
}

#[async_trait::async_trait]
impl DataParser for DelimitedDataParser {
    type Options = DelimitedDataOptions;

    fn new(options: Self::Options) -> BulkDataResult<Self>
    where
        Self: Sized,
    {
        Ok(Self { options })
    }

    fn options(&self) -> &Self::Options {
        &self.options
    }

    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let file_path = self.options.file_path;
        let Ok(file) = TkFile::open(&file_path).await else {
            return record_channel
                .send(Err(format!("Could not open delimited data file, {:?}", &file_path).into()))
                .await
                .err();
        };
        let reader = TkBufReader::new(file);
        let mut lines = reader.lines();
        let mut line_number = 1;
        loop {
            let Ok(line_option) = lines.next_line().await else {
                return record_channel
                    .send(Err(format!("Could not read line {}", &line_number).into()))
                    .await
                    .err();
            };
            let Some(mut line) = line_option else {
                println!("Found EOF");
                break;
            };
            line.push('\n');
            let result = record_channel.send(Ok(line)).await;
            if let Err(error) = result {
                return Some(error);
            }
            line_number += 1;
        }
        None
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
