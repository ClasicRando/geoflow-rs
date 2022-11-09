use super::{
    delimited::{DelimitedDataOptions, DelimitedSchemaParser},
    error::BulkDataResult,
    options::DataFileOptions,
};
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref SQL_NAME_REGEX: Regex = Regex::new("^[A-Z_][A-Z_0-9]{1,64}$").unwrap();
}

fn clean_sql_name(name: &str) -> Option<String> {
    lazy_static! {
        static ref SQL_NAME_CLEAN_REGEX1: Regex = Regex::new("[^A-Z_0-9]").unwrap();
        static ref SQL_NAME_CLEAN_REGEX2: Regex = Regex::new("^([0-9])").unwrap();
        static ref SQL_NAME_CLEAN_REGEX3: Regex = Regex::new("_{2,}").unwrap();
    }
    let name = SQL_NAME_CLEAN_REGEX1.replace(name, "_");
    let name = SQL_NAME_CLEAN_REGEX2.replace(&name, "_$1");
    let name = SQL_NAME_CLEAN_REGEX2.replace(&name, "_");
    if name.is_empty() {
        return None;
    }
    Some(name.to_string())
}

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
    index: usize,
    column_type: ColumnType,
}

impl ColumnMetadata {
    pub fn new(name: String, index: usize, column_type: ColumnType) -> BulkDataResult<Self> {
        if SQL_NAME_REGEX.is_match(&name) {
            return Ok(Self {
                name: name.to_owned(),
                index,
                column_type,
            });
        }
        let Some(column_name) = clean_sql_name(&name) else {
            return Err(format!("Column name for index {} was empty after cleaning", index).into());
        };
        Ok(Self {
            name: column_name,
            index,
            column_type,
        })
    }
}

pub struct Schema {
    table_name: String,
    columns: Vec<ColumnMetadata>,
}

impl Schema {
    pub fn new(table_name: &str, columns: Vec<ColumnMetadata>) -> BulkDataResult<Self> {
        if SQL_NAME_REGEX.is_match(table_name) {
            return Ok(Self {
                table_name: table_name.to_owned(),
                columns,
            });
        }
        let Some(table_name) = clean_sql_name(table_name) else {
            return Err(format!("Table Name {} was empty after cleaning", table_name).into());
        };
        Ok(Self {
            table_name,
            columns,
        })
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

impl SchemaAnalyzer<DelimitedSchemaParser> {
    pub fn new(options: DelimitedDataOptions) -> Self {
        Self(DelimitedSchemaParser::new(options))
    }
}

impl<P: SchemaParser> SchemaAnalyzer<P> {
    pub fn schema(&self) -> BulkDataResult<Schema> {
        self.0.schema()
    }
}
