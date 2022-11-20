use super::{
    analyze::{ColumnMetadata, ColumnType, Schema, SchemaParser},
    error::BulkDataResult,
    load::{DataLoader, DataParser},
    options::DataFileOptions,
};
use std::path::PathBuf;
use tokio::{
    fs::File as TkFile,
    io::{AsyncBufReadExt, BufReader as TkBufReader, Lines as TkLines},
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

    async fn async_lines(&self) -> BulkDataResult<TkLines<TkBufReader<TkFile>>> {
        let file = TkFile::open(&self.file_path).await?;
        let reader = TkBufReader::new(file);
        Ok(reader.lines())
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

pub struct DelimitedSchemaParser(DelimitedDataOptions);

#[async_trait::async_trait]
impl SchemaParser for DelimitedSchemaParser {
    type Options = DelimitedDataOptions;
    type DataParser = DelimitedDataParser;

    fn new(options: Self::Options) -> Self
    where
        Self: Sized,
    {
        Self(options)
    }

    async fn schema(&self) -> BulkDataResult<Schema> {
        let Some(table_name) = self.0.file_path.file_name().and_then(|f| f.to_str()) else {
            return Err(format!("Could not get filename for \"{:?}\"", &self.0.file_path).into())
        };
        let Ok(mut lines) = self.0.async_lines().await else {
            return Err(format!("Could not get lines from \"{:?}\"", &self.0.file_path).into())
        };
        let Ok(Some(header_line)) = lines.next_line().await else {
            return Err(format!("Could not get first line of \"{:?}\"", &self.0.file_path).into())
        };
        let columns: Vec<ColumnMetadata> = header_line
            .split(self.0.delimiter)
            .enumerate()
            .map(|(i, field)| ColumnMetadata::new(field.trim_matches('"'), i, ColumnType::Text))
            .collect::<BulkDataResult<_>>()?;
        Schema::new(table_name, columns)
    }

    fn data_loader(self) -> DataLoader<Self::DataParser> {
        let options = self.0;
        let parser = DelimitedDataParser::new(options);
        DataLoader::new(parser)
    }
}

pub struct DelimitedDataParser(DelimitedDataOptions);

impl DelimitedDataParser {
    pub fn new(options: DelimitedDataOptions) -> Self {
        Self(options)
    }
}

#[async_trait::async_trait]
impl DataParser for DelimitedDataParser {
    type Options = DelimitedDataOptions;

    fn options(&self) -> &Self::Options {
        &self.0
    }

    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let options = self.0;
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
}
