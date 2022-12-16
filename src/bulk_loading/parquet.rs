use super::{
    analyze::{ColumnType, Schema},
    error::BulkDataResult,
    load::{csv_result_iter_to_string, RecordSpoolChannel, RecordSpoolResult},
    options::DataOptions,
};
use parquet::{
    basic::{LogicalType, Type as PhysicalType},
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
    record::Field,
};
use serde::{Deserialize, Serialize};
use std::{fs::File, path::PathBuf, sync::Arc};
use wkb::wkb_to_geom;
use wkt::ToWkt;

#[derive(Deserialize, Serialize)]
pub struct ParquetFileOptions {
    file_path: PathBuf,
}

impl ParquetFileOptions {
    pub fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }

    pub fn reader(&self) -> BulkDataResult<SerializedFileReader<File>> {
        let file = File::open(&self.file_path)?;
        let reader = SerializedFileReader::new(file)?;
        Ok(reader)
    }
}

impl DataOptions for ParquetFileOptions {}

impl From<&Arc<parquet::schema::types::Type>> for ColumnType {
    fn from(field: &Arc<parquet::schema::types::Type>) -> Self {
        match field.get_basic_info().logical_type() {
            Some(LogicalType::String) => ColumnType::Text,
            Some(LogicalType::Map) => ColumnType::Json,
            Some(LogicalType::List) => ColumnType::Json,
            Some(LogicalType::Enum) => ColumnType::Text,
            Some(LogicalType::Decimal { .. }) => ColumnType::DoublePrecision,
            Some(LogicalType::Date) => ColumnType::Date,
            Some(LogicalType::Time { .. }) => ColumnType::Time,
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                ..
            }) => {
                if is_adjusted_to_u_t_c {
                    ColumnType::Timestamp
                } else {
                    ColumnType::TimestampWithZone
                }
            }
            Some(LogicalType::Bson) => ColumnType::Json,
            Some(LogicalType::Json) => ColumnType::Json,
            Some(LogicalType::Uuid) => ColumnType::UUID,
            _ => match field.get_physical_type() {
                PhysicalType::BOOLEAN => ColumnType::Boolean,
                PhysicalType::INT32 => ColumnType::Integer,
                PhysicalType::INT64 => ColumnType::BigInt,
                PhysicalType::INT96 => ColumnType::BigInt,
                PhysicalType::FLOAT => ColumnType::Real,
                PhysicalType::DOUBLE => ColumnType::DoublePrecision,
                PhysicalType::BYTE_ARRAY => {
                    if field.name() == "geometry" {
                        ColumnType::Geometry
                    } else {
                        ColumnType::Text
                    }
                }
                PhysicalType::FIXED_LEN_BYTE_ARRAY => ColumnType::Text,
            },
        }
    }
}

pub fn schema(options: &ParquetFileOptions) -> BulkDataResult<Schema> {
    let Some(table_name) = options.file_path.file_name().and_then(|f| f.to_str()) else {
        return Err(format!("Could not get filename for \"{:?}\"", &options.file_path).into())
    };
    let reader = options.reader()?;
    let columns = reader
        .metadata()
        .file_metadata()
        .schema()
        .get_fields()
        .iter()
        .map(|field| {
            let name = field.name();
            let actual_type = field.into();
            (name, actual_type)
        });
    Schema::from_iter(table_name, columns)
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
        Field::Group(_) | Field::ListInternal(_) | Field::MapInternal(_) => {
            format!("{}", field.to_json_value())
        }
        Field::Str(s) => s.to_string(),
        _ => field.to_string(),
    })
}

pub async fn spool_records(
    options: &ParquetFileOptions,
    record_channel: &mut RecordSpoolChannel,
) -> RecordSpoolResult {
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
