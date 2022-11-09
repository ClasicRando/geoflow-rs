use super::{error::BulkDataResult, options::DataFileOptions};

pub enum ColumnType {
    Text,
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Number,
    Float,
    DoublePrecision,
    Money,
    Timestamp,
    TimestampWithZone,
    Date,
    Time,
    Interval,
    Geometry,
    Json,
}

impl ColumnType {
    pub fn pg_name(&self) -> &'static str {
        match self {
            ColumnType::Text => "text",
            ColumnType::Boolean => "boolean",
            ColumnType::SmallInt => "smallint",
            ColumnType::Integer => "integer",
            ColumnType::BigInt => "bigint",
            ColumnType::Number => "numeric",
            ColumnType::Float => "float",
            ColumnType::DoublePrecision => "double precision",
            ColumnType::Money => "money",
            ColumnType::Timestamp => "timestamp without time zone",
            ColumnType::TimestampWithZone => "timestamp with time zone",
            ColumnType::Date => "date",
            ColumnType::Time => "time",
            ColumnType::Interval => "interval",
            ColumnType::Geometry => "geometry",
            ColumnType::Json => "jsonb",
        }
    }
}

pub struct ColumnMetadata {
    name: String,
    column_type: ColumnType,
}

pub struct Schema {
    table_name: String,
    columns: Vec<ColumnMetadata>,
}

pub trait SchemaParser {
    type Options: DataFileOptions;

    fn new(options: Self::Options) -> BulkDataResult<Self>
    where
        Self: Sized;
    fn schema(&self) -> BulkDataResult<Schema>;
}

pub struct SchemaAnalyzer<P: SchemaParser>(P);

impl<P: SchemaParser> SchemaAnalyzer<P> {
    pub fn schema(&self) -> BulkDataResult<Schema> {
        self.0.schema()
    }
}
