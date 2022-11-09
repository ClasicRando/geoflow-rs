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

impl ColumnMetadata {
    pub fn new(name: &str, column_type: ColumnType) -> Self {
        Self {
            name: name.to_owned(),
            column_type,
        }
    }
}

pub struct Schema {
    table_name: String,
    columns: Vec<ColumnMetadata>,
}

impl Schema {
    pub fn new(table_name: &str, columns: Vec<ColumnMetadata>) -> Self {
        Self {
            table_name: table_name.to_owned(),
            columns,
        }
    }
}

pub trait SchemaParser {
    type Options: DataFileOptions;

    fn new(options: Self::Options) -> Self
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
