use super::{
    analyze::{Schema, SchemaParser, ColumnType, ColumnMetadata},
    error::BulkDataResult,
    load::{csv_result_iter_to_string, DataLoader, DataParser},
    options::DataFileOptions,
    // utilities::{schema_from_dataframe, spool_dataframe_records},
};
use parquet::{
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
    record::Field,
    basic::Type as PhysicalType,
};
use polars::prelude::{DataFrame, ParquetReader, SerReader};
use std::{fs::File, path::PathBuf};
use tokio::sync::mpsc::{error::SendError, Sender};
use wkb::wkb_to_geom;
use wkt::ToWkt;

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

    pub fn reader(&self) -> BulkDataResult<SerializedFileReader<File>> {
        let file = File::open(&self.file_path)?;
        let reader = SerializedFileReader::new(file)?;
        Ok(reader)
    }
}

impl DataFileOptions for ParquetFileOptions {}

impl From<(&str, PhysicalType)> for ColumnType {
    fn from(t: (&str, PhysicalType)) -> Self {
        match t.1 {
            PhysicalType::BOOLEAN => ColumnType::Boolean,
            PhysicalType::INT32 => ColumnType::Integer,
            PhysicalType::INT64 => ColumnType::BigInt,
            PhysicalType::INT96 => ColumnType::BigInt,
            PhysicalType::FLOAT => ColumnType::Real,
            PhysicalType::DOUBLE => ColumnType::DoublePrecision,
            PhysicalType::BYTE_ARRAY => {
                if t.0 == "geometry" {
                    ColumnType::Geometry
                } else {
                    ColumnType::Text
                }
            }
            PhysicalType::FIXED_LEN_BYTE_ARRAY => ColumnType::Text,
        }
    }
}

pub struct ParquetSchemaParser(ParquetFileOptions);

impl SchemaParser for ParquetSchemaParser {
    type Options = ParquetFileOptions;
    type DataParser = ParquetFileParser;

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
        let reader = self.0.reader()?;
        let columns: Vec<ColumnMetadata> = reader
            .metadata()
            .file_metadata()
            .schema()
            .get_fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let name = field.name();
                let actual_type = (name, field.get_physical_type()).into();
                ColumnMetadata::new(name, i, actual_type)
            })
            .collect::<BulkDataResult<_>>()?;
        Ok(Schema::new(table_name, columns)?)
    }

    fn data_loader(self) -> DataLoader<Self::DataParser> {
        let options = self.0;
        let parser = ParquetFileParser::new(options);
        DataLoader::new(parser)
    }
}

fn map_parquet_field(name: &String, field: &Field) -> BulkDataResult<String> {
    Ok(match field {
        Field::Null => String::new(),
        Field::Bytes(b) => {
            if name == "geometry" {
                wkb_to_geom(&mut b.data())?.wkt_string()
            } else {
                format!("{}", b)
            }
        }
        Field::Group(_) | Field::ListInternal(_) | Field::MapInternal(_) => format!("{}", field.to_json_value()),
        Field::Str(s) => s.to_string(),
        _ => field.to_string(),
    })
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
        let reader = match options.reader() {
            Ok(r) => r,
            Err(error) => return record_channel.send(Err(error)).await.err(),
        };
        let iter = match reader.get_row_iter(None) {
            Ok(iter) => iter,
            Err(error) => return record_channel.send(Err(error.into())).await.err(),
        };
        for row in iter {
            let csv_iter = row
                .get_column_iter()
                .map(|(name, field)| map_parquet_field(name, field));
            let result = record_channel
                .send(csv_result_iter_to_string(csv_iter))
                .await;
            if let Err(error) = result {
                return Some(error);
            }
        }
        None
    }
}
