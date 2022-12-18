use super::{
    analyze::{ColumnType, Schema},
    error::BulkDataResult,
    load::{RecordSpoolChannel, RecordSpoolResult},
    options::DataOptions,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::{
    fs::File as TkFile,
    io::{AsyncBufReadExt, BufReader as TkBufReader, Lines as TkLines},
};

#[derive(Deserialize, Serialize)]
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

    async fn async_lines(&self) -> BulkDataResult<TkLines<TkBufReader<TkFile>>> {
        let file = TkFile::open(&self.file_path).await?;
        let reader = TkBufReader::new(file);
        Ok(reader.lines())
    }
}

impl DataOptions for DelimitedDataOptions {
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

pub async fn schema(options: &DelimitedDataOptions) -> BulkDataResult<Schema> {
    let Some(table_name) = options.file_path.file_name().and_then(|f| f.to_str()) else {
        return Err(format!("Could not get filename for \"{:?}\"", &options.file_path).into())
    };
    let Ok(mut lines) = options.async_lines().await else {
        return Err(format!("Could not get lines from \"{:?}\"", &options.file_path).into())
    };
    let Ok(Some(header_line)) = lines.next_line().await else {
        return Err(format!("Could not get first line of \"{:?}\"", &options.file_path).into())
    };
    let columns = header_line
        .split(options.delimiter)
        .map(|field| (field.trim_matches('"'), ColumnType::Text));
    Schema::from_iter(table_name, columns)
}

pub async fn spool_records(
    options: &DelimitedDataOptions,
    record_channel: &mut RecordSpoolChannel,
) -> RecordSpoolResult {
    let file_path = &options.file_path;
    let Ok(mut lines) = options.async_lines().await else {
        return record_channel
            .send(Err(format!("Could not open delimited data file, {:?}", file_path).into()))
            .await
            .err();
    };
    let mut line_number = 1;
    loop {
        let Ok(line_option) = lines.next_line().await else {
            return record_channel
                .send(Err(format!("Could not read line {}", &line_number).into()))
                .await
                .err();
        };
        let Some(mut line) = line_option else {
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
