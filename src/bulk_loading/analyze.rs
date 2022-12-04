use super::{
    arcgis::{ArcGisDataOptions, ArcGisRestSchemaParser},
    avro::{AvroFileOptions, AvroSchemaParser},
    delimited::{DelimitedDataOptions, DelimitedSchemaParser},
    error::{BulkDataError, BulkDataResult},
    excel::{ExcelOptions, ExcelSchemaParser},
    geo_json::{GeoJsonOptions, GeoJsonSchemaParser},
    ipc::{IpcFileOptions, IpcSchemaParser},
    load::{CopyOptions, DataLoader, DataParser},
    options::DataOptions,
    parquet::{ParquetFileOptions, ParquetSchemaParser},
    shape::{ShapeDataOptions, ShapeDataSchemaParser},
};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::{Regex, RegexBuilder};
use serde::{Serialize, Deserialize};
use sqlx::postgres::{PgHasArrayType, PgTypeInfo};

lazy_static! {
    static ref SQL_NAME_REGEX: Regex = Regex::new("^[A-Z_][A-Z_0-9]{1,64}$").unwrap();
}

fn clean_sql_name(name: &str) -> Option<String> {
    lazy_static! {
        static ref SQL_NAME_CLEAN_REGEX1: Regex = Regex::new("\\..+$").unwrap();
        static ref SQL_NAME_CLEAN_REGEX2: Regex = RegexBuilder::new("[^A-Z_0-9]")
            .case_insensitive(true)
            .build()
            .unwrap();
        static ref SQL_NAME_CLEAN_REGEX3: Regex = Regex::new("^([0-9])").unwrap();
        static ref SQL_NAME_CLEAN_REGEX4: Regex = Regex::new("_{2,}").unwrap();
    }
    let name = SQL_NAME_CLEAN_REGEX1.replace(name, "");
    let name = SQL_NAME_CLEAN_REGEX2.replace_all(&name, "_");
    let name = SQL_NAME_CLEAN_REGEX3.replace(&name, "_$1");
    let name = SQL_NAME_CLEAN_REGEX4.replace_all(&name, "_");
    if name.is_empty() {
        return None;
    }
    Some(name.to_lowercase())
}

#[derive(Debug, PartialEq, Eq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "column_type")]
pub enum ColumnType {
    Text,
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Number,
    Real,
    DoublePrecision,
    Money,
    Timestamp,
    TimestampWithZone,
    Date,
    Time,
    Interval,
    Geometry,
    Json,
    UUID,
    SmallIntArray,
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
            ColumnType::Real => "real",
            ColumnType::DoublePrecision => "double precision",
            ColumnType::Money => "money",
            ColumnType::Timestamp => "timestamp without time zone",
            ColumnType::TimestampWithZone => "timestamp with time zone",
            ColumnType::Date => "date",
            ColumnType::Time => "time",
            ColumnType::Interval => "interval",
            ColumnType::Geometry => "geometry",
            ColumnType::Json => "jsonb",
            ColumnType::UUID => "uuid",
            ColumnType::SmallIntArray => "smallint[]",
        }
    }
}

#[derive(Debug, sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "column_metadata")]
pub struct ColumnMetadata {
    name: String,
    column_type: ColumnType,
}

impl ColumnMetadata {
    pub fn new(name: &str, column_type: ColumnType) -> BulkDataResult<Self> {
        if SQL_NAME_REGEX.is_match(name) {
            return Ok(Self {
                name: name.to_lowercase(),
                column_type,
            });
        }
        let Some(column_name) = clean_sql_name(name) else {
            return Err(format!("Column name \"{}\" was empty after cleaning", name).into());
        };
        Ok(Self {
            name: column_name,
            column_type,
        })
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn column_type(&self) -> &ColumnType {
        &self.column_type
    }
}

impl PgHasArrayType for ColumnMetadata {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_column_metadata")
    }
}

impl TryFrom<(String, Option<ColumnType>)> for ColumnMetadata {
    type Error = BulkDataError;

    fn try_from(value: (String, Option<ColumnType>)) -> Result<Self, Self::Error> {
        Self::new(&value.0, value.1.unwrap_or(ColumnType::Text))
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Schema {
    table_name: String,
    columns: Vec<ColumnMetadata>,
}

impl Schema {
    pub fn new(table_name: &str, columns: Vec<ColumnMetadata>) -> BulkDataResult<Self> {
        if SQL_NAME_REGEX.is_match(table_name) {
            return Ok(Self {
                table_name: table_name.to_lowercase(),
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

    pub fn from_iter<S: AsRef<str>, I: Iterator<Item = (S, ColumnType)>>(
        table_name: &str,
        columns: I,
    ) -> BulkDataResult<Self> {
        let columns = columns
            .map(|(name, typ)| ColumnMetadata::new(name.as_ref(), typ))
            .collect::<BulkDataResult<_>>()?;
        Self::new(table_name, columns)
    }

    pub fn from_result_iter<S: AsRef<str>, I: Iterator<Item = BulkDataResult<(S, ColumnType)>>>(
        table_name: &str,
        columns: I,
    ) -> BulkDataResult<Self> {
        let columns = columns
            .map(|item| -> BulkDataResult<ColumnMetadata> {
                let (name, typ) = item?;
                ColumnMetadata::new(name.as_ref(), typ)
            })
            .collect::<BulkDataResult<_>>()?;
        Self::new(table_name, columns)
    }

    pub fn copy_options(&self, db_schema: &str) -> CopyOptions {
        CopyOptions::from_vec(
            format!("{}.\"{}\"", db_schema, self.table_name),
            self.columns.iter().map(|c| c.name().to_owned()).collect(),
        )
    }

    pub fn create_statement(&self, db_schema: &str) -> String {
        format!(
            "create table {}.\"{}\"({})",
            db_schema,
            &self.table_name,
            self.columns
                .iter()
                .map(|c| format!("\"{}\" {}", &c.name, c.column_type.pg_name()))
                .join(",")
        )
    }

    #[inline]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    #[inline]
    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }
}

#[async_trait::async_trait]
pub trait SchemaParser {
    type Options: DataOptions;
    type DataParser: DataParser + Send + Sync;

    fn new(options: Self::Options) -> Self
    where
        Self: Sized;
    async fn schema(&self) -> BulkDataResult<Schema>;
    fn data_loader(self) -> DataLoader<Self::DataParser>;
}

pub struct SchemaAnalyzer<P: SchemaParser>(P);

impl SchemaAnalyzer<DelimitedSchemaParser> {
    pub fn from_delimited(options: DelimitedDataOptions) -> Self {
        Self(DelimitedSchemaParser::new(options))
    }
}

impl SchemaAnalyzer<ExcelSchemaParser> {
    pub fn from_excel(options: ExcelOptions) -> Self {
        Self(ExcelSchemaParser::new(options))
    }
}

impl SchemaAnalyzer<GeoJsonSchemaParser> {
    pub fn from_geo_json(options: GeoJsonOptions) -> Self {
        Self(GeoJsonSchemaParser::new(options))
    }
}

impl SchemaAnalyzer<ShapeDataSchemaParser> {
    pub fn from_shapefile(options: ShapeDataOptions) -> Self {
        Self(ShapeDataSchemaParser::new(options))
    }
}

impl SchemaAnalyzer<ParquetSchemaParser> {
    pub fn from_parquet(options: ParquetFileOptions) -> Self {
        Self(ParquetSchemaParser::new(options))
    }
}

impl SchemaAnalyzer<IpcSchemaParser> {
    pub fn from_ipc(options: IpcFileOptions) -> Self {
        Self(IpcSchemaParser::new(options))
    }
}

impl SchemaAnalyzer<ArcGisRestSchemaParser> {
    pub fn from_arc_gis(options: ArcGisDataOptions) -> Self {
        Self(ArcGisRestSchemaParser::new(options))
    }
}

impl SchemaAnalyzer<AvroSchemaParser> {
    pub fn from_avro(options: AvroFileOptions) -> Self {
        Self(AvroSchemaParser::new(options))
    }
}

impl<P: SchemaParser> SchemaAnalyzer<P> {
    pub async fn schema(&self) -> BulkDataResult<Schema> {
        self.0.schema().await
    }

    pub fn loader(self) -> DataLoader<P::DataParser> {
        self.0.data_loader()
    }
}
