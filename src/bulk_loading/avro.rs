use super::{
    analyze::{ColumnType, Schema, SchemaParser},
    error::BulkDataResult,
    load::{
        csv_result_iter_to_string, DataLoader, DataParser, RecordSpoolChannel, RecordSpoolResult,
    },
    options::DataOptions,
};
use avro_rs::{
    schema::{RecordField, Schema as AvroSchema, UnionSchema},
    types::Value,
    Duration, Reader,
};
use chrono::{LocalResult, NaiveTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::{collections::HashSet, fmt::Write};
use std::{fs::File, io::BufReader, path::PathBuf};

#[derive(Deserialize, Serialize)]
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

impl DataOptions for AvroFileOptions {}

fn is_nullable_union_schema(schema: &UnionSchema) -> bool {
    schema.variants().len() <= 2 && schema.find_schema(&Value::Null).is_some()
}

fn avro_schema_to_column_type(schema: &AvroSchema) -> BulkDataResult<ColumnType> {
    Ok(match &schema {
        AvroSchema::Null => ColumnType::Text,
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
        AvroSchema::Duration => ColumnType::Json,
    })
}

fn avro_field_to_column_type(field: &RecordField) -> BulkDataResult<ColumnType> {
    match &field.schema {
        AvroSchema::Array(_) => Ok(ColumnType::Json),
        AvroSchema::Union(s) => {
            if is_nullable_union_schema(s) {
                let Some(schema) = s.variants().iter().find(|v| *v != &AvroSchema::Null) else {
                    return Ok(ColumnType::Json)
                };
                avro_schema_to_column_type(schema)
            } else {
                Ok(ColumnType::Json)
            }
        }
        AvroSchema::Null => Err(format!("Found a null schema for field \"{}\"", field.name).into()),
        _ => avro_schema_to_column_type(&field.schema),
    }
}

pub struct AvroSchemaParser(AvroFileOptions);

#[async_trait::async_trait]
impl SchemaParser for AvroSchemaParser {
    type Options = AvroFileOptions;
    type DataParser = AvroFileParser;

    fn new(options: AvroFileOptions) -> Self
    where
        Self: Sized,
    {
        Self(options)
    }

    async fn schema(&self) -> BulkDataResult<Schema> {
        let Some(table_name) = self.0.file_path.file_name().and_then(|f| f.to_str()) else {
            return Err(format!("Could not get filename for \"{:?}\"", &self.0.file_path).into())
        };
        let reader = self.0.reader()?;
        let AvroSchema::Record { fields, .. } = reader.writer_schema() else {
            return Err(format!("File schema for \"{:?}\" is not a record. Found {:?}", &self.0.file_path, reader.writer_schema()).into())
        };
        let columns = fields
            .iter()
            .map(|f| -> BulkDataResult<_> { Ok((&f.name, avro_field_to_column_type(f)?)) });
        Schema::from_result_iter(table_name, columns)
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
    let secs = (value - nano_overflow) / 1_000_000_000;
    NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nano_overflow as u32)
        .map(|t| format!("{}", t.format("%H:%M:%S")))
        .ok_or_else(|| format!("Could not convert {} ns to Time", value).into())
}

#[inline]
fn convert_timestamp_secs_to_string(value: i64) -> BulkDataResult<String> {
    let LocalResult::Single(dt) = Utc.timestamp_opt(value, 0) else {
        return Err(format!("Could not convert {} secs to Timestamp", value).into())
    };
    Ok(format!("{}", dt.format("%Y-%m-%d %H:%M:%S")))
}

#[inline]
fn small_int_array_literal(bytes: Vec<u8>) -> BulkDataResult<String> {
    let mut out = String::from('{');
    if !bytes.is_empty() {
        write!(out, "{}", bytes[0])?;
        for byte in bytes.iter().skip(1) {
            write!(out, ",{}", byte)?;
        }
    }
    out.push('}');
    Ok(out)
}

#[inline]
fn serialize_to_json_value(avro_value: Value) -> BulkDataResult<String> {
    let value: JsonValue = avro_value.try_into()?;
    Ok(serde_json::to_string(&value)?)
}

#[inline]
fn union_to_json_value(union: Value) -> BulkDataResult<String> {
    let union_type = match union {
        Value::Null => "null",
        Value::Boolean(_) => "boolean",
        Value::Int(_) => "int",
        Value::Long(_) => "long",
        Value::Float(_) => "float",
        Value::Double(_) => "double",
        Value::Bytes(_) => "bytes",
        Value::String(_) => "string",
        Value::Fixed(_, _) => "fixed",
        Value::Enum(_, _) => "enum",
        Value::Union(_) => "union",
        Value::Array(_) => "array",
        Value::Map(_) => "map",
        Value::Record(_) => "record",
        Value::Date(_) => "date",
        Value::Decimal(_) => "decimal",
        Value::TimeMillis(_) => "time_millis",
        Value::TimeMicros(_) => "time_micros",
        Value::TimestampMillis(_) => "timestamp_millis",
        Value::TimestampMicros(_) => "timestamp_micros",
        Value::Duration(_) => "duration",
        Value::Uuid(_) => "uuid",
    };
    let union_value = map_avro_value(union)?;
    Ok(json!({
        "type": union_type,
        "value": union_value,
    })
    .to_string())
}

#[inline]
fn duration_to_json_value(duration: Duration) -> String {
    let months = u32::from_le_bytes(*duration.months().as_ref());
    let days = u32::from_le_bytes(*duration.days().as_ref());
    let millis = u32::from_le_bytes(*duration.millis().as_ref());
    json!({
        "months": months,
        "days": days,
        "millis": millis,
    })
    .to_string()
}

fn map_avro_value(value: Value) -> BulkDataResult<String> {
    Ok(match value {
        Value::Null => String::new(),
        Value::Boolean(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Long(l) => l.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(d) => d.to_string(),
        Value::Bytes(b) => small_int_array_literal(b)?,
        Value::String(s) => s,
        Value::Fixed(_, b) => small_int_array_literal(b)?,
        Value::Enum(_, n) => n,
        Value::Union(b) => union_to_json_value(*b)?,
        Value::Record(_) | Value::Map(_) | Value::Array(_) => serialize_to_json_value(value)?,
        Value::Date(d) => {
            static NUM_SECONDS_IN_DAY: i64 = 60 * 60 * 24;
            let LocalResult::Single(dt) = Utc.timestamp_opt(d as i64 * NUM_SECONDS_IN_DAY, 0) else {
                return Err(format!("Could not convert {} days to Timestamp", d).into())
            };
            format!("{}", dt.format("%Y-%m-%d"))
        }
        Value::Decimal(ref d) => small_int_array_literal(d.try_into()?)?,
        Value::TimeMillis(t) => convert_time_nano_secs_to_string(t as i64 * 1_000_000)?,
        Value::TimeMicros(t) => convert_time_nano_secs_to_string(t as i64 * 1_000)?,
        Value::TimestampMillis(t) => convert_timestamp_secs_to_string(t as i64 / 1_000)?,
        Value::TimestampMicros(t) => convert_timestamp_secs_to_string(t as i64 / 1_000_000)?,
        Value::Duration(d) => duration_to_json_value(d),
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

    async fn spool_records(self, record_channel: &mut RecordSpoolChannel) -> RecordSpoolResult {
        let options = self.0;
        let reader = match options.reader() {
            Ok(reader) => reader,
            Err(error) => return record_channel.send(Err(error)).await.err(),
        };
        let AvroSchema::Record { fields, .. } = reader.writer_schema() else {
            return record_channel.send(
                Err(
                    format!(
                        "File schema for \"{:?}\" is not a record. Found {:?}",
                        &options.file_path,
                        reader.writer_schema()
                    )
                    .into()
                )
            )
            .await
            .err()
        };
        let nullable_union_columns: HashSet<String> = fields
            .iter()
            .filter_map(|f| {
                if let AvroSchema::Union(schema) = &f.schema {
                    if is_nullable_union_schema(schema) {
                        return Some(f.name.to_owned());
                    }
                }
                None
            })
            .collect();
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
            let csv_iter = record.into_iter().map(|(key, value)| {
                if nullable_union_columns.contains(&key) {
                    if let Value::Union(union_box) = value {
                        return map_avro_value(*union_box);
                    }
                }
                map_avro_value(value)
            });
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

#[cfg(test)]
mod tests {
    use super::avro_field_to_column_type;
    use crate::bulk_loading::{analyze::ColumnType, avro::map_avro_value, error::BulkDataResult};
    use avro_rs::{
        schema::{Name, RecordField, RecordFieldOrder},
        types::Value,
        Days, Duration, Millis, Months, Schema as AvroSchema,
    };
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use polars::export::num::{BigInt, One};
    use serde_json::{json, Value as JsonValue};
    use std::collections::HashMap;

    fn record_field_for_type(schema: AvroSchema) -> RecordField {
        RecordField {
            name: String::from("test"),
            doc: None,
            default: None,
            schema,
            order: RecordFieldOrder::Ignore,
            position: 1,
        }
    }

    #[test]
    fn avro_field_to_column_type_should_fail_when_fail_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Null;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field);

        assert!(column_type.is_err());

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_boolean_when_boolean_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Boolean;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Boolean, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_integer_when_int_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Int;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Integer, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_bigint_when_long_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Long;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::BigInt, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_real_when_float_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Float;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Real, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_double_precision_when_double_type(
    ) -> BulkDataResult<()> {
        let schema = AvroSchema::Double;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::DoublePrecision, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_smallint_array_when_bytes_type() -> BulkDataResult<()>
    {
        let schema = AvroSchema::Bytes;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::SmallIntArray, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_json_when_array_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Array(Box::new(AvroSchema::Int));
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Json, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_json_when_map_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Map(Box::new(AvroSchema::Int));
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Json, column_type);

        Ok(())
    }

    #[test]
    #[ignore = "Union schema cannot be created since it's crate private"]
    fn avro_field_to_column_type_should_return_json_when_union_type() -> BulkDataResult<()> {
        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_json_when_record_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Record {
            name: Name::new("Test"),
            doc: None,
            fields: vec![record_field_for_type(AvroSchema::Int)],
            lookup: HashMap::new(),
        };
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Json, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_text_when_enum_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Enum {
            name: Name::new("Test"),
            doc: None,
            symbols: vec![],
        };
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Text, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_smallint_array_when_fixed_type() -> BulkDataResult<()>
    {
        let schema = AvroSchema::Fixed {
            name: Name::new("Test"),
            size: 0,
        };
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::SmallIntArray, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_smallint_array_when_decimal_type(
    ) -> BulkDataResult<()> {
        let schema = AvroSchema::Decimal {
            precision: 0,
            scale: 0,
            inner: Box::new(AvroSchema::Int),
        };
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::SmallIntArray, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_text_when_uuid_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Uuid;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::UUID, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_date_when_date_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Date;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Date, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_time_when_time_milli_type() -> BulkDataResult<()> {
        let schema = AvroSchema::TimeMillis;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Time, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_time_when_time_micro_type() -> BulkDataResult<()> {
        let schema = AvroSchema::TimeMicros;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Time, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_timestamp_when_timestamp_millis_type(
    ) -> BulkDataResult<()> {
        let schema = AvroSchema::TimestampMillis;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Timestamp, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_timestamp_when_timestamp_micros_type(
    ) -> BulkDataResult<()> {
        let schema = AvroSchema::TimestampMicros;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Timestamp, column_type);

        Ok(())
    }

    #[test]
    fn avro_field_to_column_type_should_return_text_when_duration_type() -> BulkDataResult<()> {
        let schema = AvroSchema::Duration;
        let field = record_field_for_type(schema);

        let column_type = avro_field_to_column_type(&field)?;

        assert_eq!(ColumnType::Json, column_type);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_empty_string_when_null_value() -> BulkDataResult<()> {
        let value = Value::Null;

        let result = map_avro_value(value)?;

        assert_eq!("", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_literal_bool_when_boolean_value() -> BulkDataResult<()> {
        let true_value = Value::Boolean(true);
        let false_value = Value::Boolean(false);

        let true_result = map_avro_value(true_value)?;
        let false_result = map_avro_value(false_value)?;

        assert_eq!("true", true_result);
        assert_eq!("false", false_result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_int_literal_when_int_value() -> BulkDataResult<()> {
        let value = Value::Int(26);

        let result = map_avro_value(value)?;

        assert_eq!("26", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_long_literal_when_long_value() -> BulkDataResult<()> {
        let value = Value::Long(56895645789);

        let result = map_avro_value(value)?;

        assert_eq!("56895645789", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_float_literal_when_float_value() -> BulkDataResult<()> {
        let value = Value::Float(56.2356);

        let result = map_avro_value(value)?;

        assert_eq!("56.2356", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_double_literal_when_double_value() -> BulkDataResult<()> {
        let value = Value::Double(7584259.895467);

        let result = map_avro_value(value)?;

        assert_eq!("7584259.895467", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_array_literal_when_bytes_value() -> BulkDataResult<()> {
        let value = Value::Bytes(vec![26, 85, 96]);

        let result = map_avro_value(value)?;

        assert_eq!("{26,85,96}", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_exact_string_when_string_value() -> BulkDataResult<()> {
        let str = "This is a test";
        let value = Value::String(String::from(str));

        let result = map_avro_value(value)?;

        assert_eq!(str, result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_array_literal_when_fixed_value() -> BulkDataResult<()> {
        let value = Value::Fixed(0, vec![86, 96, 84]);

        let result = map_avro_value(value)?;

        assert_eq!("{86,96,84}", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_exact_string_when_enum_value() -> BulkDataResult<()> {
        let str = "This is a test";
        let value = Value::Enum(1, String::from(str));

        let result = map_avro_value(value)?;

        assert_eq!(str, result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_inner_as_json_when_union_value() -> BulkDataResult<()> {
        let str = "This is a test";
        let expected_value = json!({
            "type": "string",
            "value": str,
        })
        .to_string();
        let value = Value::Union(Box::new(Value::String(String::from(str))));

        let result = map_avro_value(value)?;

        assert_eq!(expected_value, result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_json_string_when_array_value() -> BulkDataResult<()> {
        let arr = vec![
            Value::Int(5),
            Value::Int(6),
            Value::Int(9),
            Value::Int(8),
            Value::Int(45),
        ];
        let value = Value::Array(arr);

        let result = map_avro_value(value)?;

        assert_eq!("[5,6,9,8,45]", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_json_string_when_map_value() -> BulkDataResult<()> {
        let id = "id";
        let id_value = 8;
        let name = "name";
        let name_value = "Test";
        let typ = "type";
        let items = "items";
        let items_value = vec![5, 6];
        let expected_result = json!({
            id: id_value,
            name: name_value,
            typ: JsonValue::Null,
            items: items_value,
        });
        let obj = HashMap::from_iter(vec![
            (String::from(id), Value::Int(id_value)),
            (String::from(name), Value::String(String::from(name_value))),
            (String::from(typ), Value::Null),
            (
                String::from(items),
                Value::Array(items_value.into_iter().map(Value::Int).collect()),
            ),
        ]);
        let value = Value::Map(obj);

        let result: JsonValue = serde_json::from_str(&map_avro_value(value)?)?;

        assert_eq!(expected_result, result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_json_string_when_record_value() -> BulkDataResult<()> {
        let id = "id";
        let id_value = 8;
        let name = "name";
        let name_value = "Test";
        let typ = "type";
        let items = "items";
        let items_value = vec![5, 6];
        let expected_result = json!({
            id: id_value,
            name: name_value,
            typ: JsonValue::Null,
            items: items_value,
        });
        let obj = vec![
            (String::from(id), Value::Int(id_value)),
            (String::from(name), Value::String(String::from(name_value))),
            (String::from(typ), Value::Null),
            (
                String::from(items),
                Value::Array(items_value.into_iter().map(Value::Int).collect()),
            ),
        ];
        let value = Value::Record(obj);

        let result: JsonValue = serde_json::from_str(&map_avro_value(value)?)?;

        assert_eq!(expected_result, result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_formatted_date_when_date_value() -> BulkDataResult<()> {
        let Some(epoch_date) = NaiveDate::from_ymd_opt(1970, 1, 1) else {
            return Err("Could not create a date for epoch. This should never fail".into())
        };
        let Some(date) = NaiveDate::from_ymd_opt(2000, 1, 1) else {
            return Err("Could not create a date for Jan 1, 2000. This should never fail".into())
        };
        let value = Value::Date(date.signed_duration_since(epoch_date).num_days() as i32);

        let result = map_avro_value(value)?;

        assert_eq!("2000-01-01", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_array_literal_when_decimal_value() -> BulkDataResult<()> {
        let decimal = BigInt::one();
        let value = Value::Decimal(decimal.to_signed_bytes_be().into());

        let result = map_avro_value(value)?;

        assert_eq!("{1}", result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_formatted_time_when_time_value() -> BulkDataResult<()> {
        static SECS_IN_HOUR: i32 = 60 * 60;
        static SECS_IN_MINUTE: i32 = 60;
        let hours = 5;
        let minutes = 30;
        let secs = 5;
        let expected_result = format!("{:02}:{:02}:{:02}", hours, minutes, secs);

        let time = hours * SECS_IN_HOUR + minutes * SECS_IN_MINUTE + secs;

        let millis_value = Value::TimeMillis(time * 1_000);
        let micros_value = Value::TimeMicros(time as i64 * 1_000_000_i64);

        let millis_result = map_avro_value(millis_value)?;
        let micros_result = map_avro_value(micros_value)?;

        assert_eq!(expected_result, millis_result);
        assert_eq!(expected_result, micros_result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_formatted_timestamp_when_timestamp_value() -> BulkDataResult<()>
    {
        let expected_result = "2000-01-01 05:30:05";
        let Some(date) = NaiveDate::from_ymd_opt(2000, 1, 1) else {
            return Err("Could not create a date for Jan 1, 2000. This should never fail".into())
        };
        let Some(time) = NaiveTime::from_hms_opt(5, 30, 5) else {
            return Err("Could not create a time for 05:30:05. This should never fail".into())
        };
        let date_time = NaiveDateTime::new(date, time);

        let millis_value = Value::TimestampMillis(date_time.timestamp_millis());
        let micros_value = Value::TimestampMicros(date_time.timestamp_micros());

        let millis_result = map_avro_value(millis_value)?;
        let micros_result = map_avro_value(micros_value)?;

        assert_eq!(expected_result, millis_result);
        assert_eq!(expected_result, micros_result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_debug_output_when_duration_value() -> BulkDataResult<()> {
        let value = Value::Duration(Duration::new(
            Months::new(1),
            Days::new(5),
            Millis::new(1000),
        ));

        let result = map_avro_value(value)?;

        assert_eq!(r#"{"months":1,"days":5,"millis":1000}"#, result);

        Ok(())
    }

    #[test]
    fn map_avro_value_should_return_string_when_uuid_value() -> BulkDataResult<()> {
        let uuid_str = "a072b040-075f-4b4f-87ba-02e9e8a5622d";
        let uuid = uuid::Uuid::parse_str(uuid_str).unwrap();
        let value = Value::Uuid(uuid);

        let result = map_avro_value(value)?;

        assert_eq!(uuid_str, result);

        Ok(())
    }
}
