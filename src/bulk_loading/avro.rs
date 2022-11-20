use super::{
    analyze::{ColumnMetadata, ColumnType, Schema, SchemaParser},
    error::BulkDataResult,
    load::{csv_result_iter_to_string, DataLoader, DataParser},
    options::DataFileOptions,
};
use avro_rs::{
    schema::{RecordField, Schema as AvroSchema},
    types::Value,
    Reader,
};
use chrono::{NaiveTime, TimeZone, Utc};
use serde_json::Value as JsonValue;
use std::fmt::Write;
use std::{fs::File, io::BufReader, path::PathBuf};
use tokio::sync::mpsc::{error::SendError, Sender};

pub struct AvroFileOptions {
    file_path: PathBuf,
}

impl AvroFileOptions {
    pub fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }

    fn reader(&self) -> BulkDataResult<Reader<BufReader<File>>> {
        let file = File::open(&self.file_path)?;
        let buf_reader = BufReader::new(file);
        let reader = Reader::new(buf_reader)?;
        Ok(reader)
    }
}

impl DataFileOptions for AvroFileOptions {}

fn avro_field_to_column_type(field: &RecordField) -> BulkDataResult<ColumnType> {
    Ok(match &field.schema {
        AvroSchema::Null => {
            return Err(format!("Found a null schema for field \"{}\"", field.name).into())
        }
        AvroSchema::Boolean => ColumnType::Boolean,
        AvroSchema::Int => ColumnType::Integer,
        AvroSchema::Long => ColumnType::BigInt,
        AvroSchema::Float => ColumnType::Real,
        AvroSchema::Double => ColumnType::DoublePrecision,
        AvroSchema::Bytes => ColumnType::SmallIntArray,
        AvroSchema::String => ColumnType::Text,
        AvroSchema::Array(_) => ColumnType::Json,
        AvroSchema::Map(_) => ColumnType::Json,
        AvroSchema::Union(_) => ColumnType::Json,
        AvroSchema::Record { .. } => ColumnType::Json,
        AvroSchema::Enum { .. } => ColumnType::Text,
        AvroSchema::Fixed { .. } => ColumnType::SmallIntArray,
        AvroSchema::Decimal { .. } => ColumnType::SmallIntArray,
        AvroSchema::Uuid => ColumnType::UUID,
        AvroSchema::Date => ColumnType::Date,
        AvroSchema::TimeMillis => ColumnType::Time,
        AvroSchema::TimeMicros => ColumnType::Time,
        AvroSchema::TimestampMillis => ColumnType::Timestamp,
        AvroSchema::TimestampMicros => ColumnType::Timestamp,
        AvroSchema::Duration => ColumnType::Text,
    })
}

pub struct IpcSchemaParser(AvroFileOptions);

impl SchemaParser for IpcSchemaParser {
    type Options = AvroFileOptions;
    type DataParser = AvroFileParser;

    fn new(options: AvroFileOptions) -> Self
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
        let AvroSchema::Record { fields, .. } = reader.writer_schema() else {
            return Err(format!("File schema for \"{:?}\" is not a record. Found {:?}", &self.0.file_path, reader.writer_schema()).into())
        };
        let columns = fields
            .iter()
            .enumerate()
            .map(|(i, f)| -> BulkDataResult<ColumnMetadata> {
                ColumnMetadata::new(&f.name, i, avro_field_to_column_type(f)?)
            })
            .collect::<BulkDataResult<_>>()?;
        Ok(Schema::new(table_name, columns)?)
    }

    fn data_loader(self) -> DataLoader<Self::DataParser> {
        let options = self.0;
        let parser = AvroFileParser::new(options);
        DataLoader::new(parser)
    }
}

#[inline]
fn convert_time_nano_secs_to_string(value: i64) -> BulkDataResult<String> {
    let nano_overflow = value % 1_000_000_000;
    let secs = value - nano_overflow;
    NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nano_overflow as u32)
        .map(|t| format!("{}", t.format("%H:%M:%S")))
        .ok_or(format!("Could not convert {} ns to Time", value).into())
}

#[inline]
fn convert_timestamp_secs_to_string(value: i64) -> String {
    let dt = Utc.timestamp(value, 0);
    format!("{}", dt.format("%Y-%m-%d %H:%M:%S"))
}

fn map_avro_value(value: Value) -> BulkDataResult<String> {
    Ok(match value {
        Value::Null => String::new(),
        Value::Boolean(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Long(l) => l.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(d) => d.to_string(),
        Value::Bytes(b) => {
            let mut out = String::from('{');
            for byte in b {
                write!(out, "{}", byte)?;
            }
            out.push('}');
            out
        }
        Value::String(s) => s.to_owned(),
        Value::Fixed(_, b) => {
            let mut out = String::from('{');
            for byte in b {
                write!(out, "{}", byte)?;
            }
            out.push('}');
            out
        }
        Value::Enum(_, n) => n.to_owned(),
        Value::Union(b) => return map_avro_value(*b),
        Value::Array(_) => {
            let arr: JsonValue = value.try_into()?;
            serde_json::to_string(&arr)?
        }
        Value::Map(_) => {
            let obj: JsonValue = value.try_into()?;
            serde_json::to_string(&obj)?
        }
        Value::Record(_) => {
            let obj: JsonValue = value.try_into()?;
            serde_json::to_string(&obj)?
        }
        Value::Date(d) => {
            static NUM_SECONDS_IN_DAY: i64 = 60 * 60 * 24;
            let dt = Utc.timestamp(d as i64 * NUM_SECONDS_IN_DAY, 0).date();
            format!("{}", dt.format("%Y-%m-%d"))
        }
        Value::Decimal(ref d) => {
            let bytes: Vec<u8> = d.try_into()?;
            let mut out = String::from('{');
            for byte in bytes {
                write!(out, "{}", byte)?;
            }
            out.push('}');
            out
        }
        Value::TimeMillis(t) => convert_time_nano_secs_to_string(t as i64 * 1_000_000)?,
        Value::TimeMicros(t) => convert_time_nano_secs_to_string(t as i64 * 1_000)?,
        Value::TimestampMillis(t) => convert_timestamp_secs_to_string(t as i64 / 1_000),
        Value::TimestampMicros(t) => convert_timestamp_secs_to_string(t as i64 / 1_000_000),
        Value::Duration(d) => format!("{:?}", d),
        Value::Uuid(u) => u.to_string(),
    })
}

pub struct AvroFileParser(AvroFileOptions);

impl AvroFileParser {
    pub fn new(options: AvroFileOptions) -> Self {
        Self(options)
    }
}

#[async_trait::async_trait]
impl DataParser for AvroFileParser {
    type Options = AvroFileOptions;

    fn options(&self) -> &Self::Options {
        &self.0
    }

    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let options = self.0;
        let reader = match options.reader() {
            Ok(reader) => reader,
            Err(error) => return record_channel.send(Err(error)).await.err(),
        };
        for (i, record) in reader.enumerate() {
            let record = match record {
                Ok(Value::Record(fields)) => fields,
                Ok(_) => {
                    return record_channel
                        .send(Err(format!(
                            "Value {} from \"{:?}\" was not a record",
                            i + 1,
                            &options.file_path
                        )
                        .into()))
                        .await
                        .err()
                }
                Err(error) => return record_channel.send(Err(error.into())).await.err(),
            };
            let csv_iter = record.into_iter().map(|(_, value)| map_avro_value(value));
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
