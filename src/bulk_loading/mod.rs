mod analyze;
mod arcgis;
mod avro;
mod delimited;
pub mod error;
mod excel;
mod geo_json;
mod ipc;
mod load;
mod options;
mod parquet;
mod shape;
mod utilities;

use std::path::Path;

use self::parquet::{
    schema as parquet_schema, spool_records as parquet_spool_records, ParquetFileOptions,
};
pub use analyze::{ColumnMetadata, ColumnType};
use analyze::Schema;
use arcgis::{schema as arc_gis_schema, spool_records as arc_gis_spool_records, ArcGisDataOptions};
use avro::{schema as avro_schema, spool_records as avro_spool_records, AvroFileOptions};
use delimited::{
    schema as delimited_schema, spool_records as delimited_spool_records, DelimitedDataOptions,
};
use error::BulkDataResult;
use excel::{schema as excel_schema, spool_records as excel_spool_records, ExcelOptions};
use geo_json::{
    schema as geo_json_schema, spool_records as geo_json_spool_records, GeoJsonOptions,
};
use ipc::{schema as ipc_schema, spool_records as ipc_spool_records, IpcFileOptions};
use load::{BulkLoadResult, CopyOptions, RecordSpoolChannel, RecordSpoolResult};
use serde_json::Value;
use shape::{schema as shape_schema, spool_records as shape_spool_records, ShapeDataOptions};
use sqlx::postgres::PgPool;
use tokio::sync::mpsc::channel as mpsc_channel;

pub enum DataLoader {
    ArcGis(ArcGisDataOptions),
    Avro(AvroFileOptions),
    Delimited(DelimitedDataOptions),
    Excel(ExcelOptions),
    GeoJson(GeoJsonOptions),
    Ipc(IpcFileOptions),
    Parquet(ParquetFileOptions),
    Shape(ShapeDataOptions),
}

impl DataLoader {
    pub fn new(options: &Value) -> BulkDataResult<Self> {
        let json_string = serde_json::to_string(options)?;
        let Some(object) = options.as_object() else {
            return Err("Source data options must be an object".into())
        };
        if object.contains_key("url") {
            let arc_gis_options: ArcGisDataOptions = serde_json::from_str(&json_string)?;
            return Ok(Self::ArcGis(arc_gis_options));
        }
        let Some(file_path) = object.get("file_path").and_then(|p| p.as_str()) else {
            return Err("Source data options must contain a string \"file_path\" property".into())
        };
        let Some(ext) = Path::new(file_path).extension().and_then(|e| e.to_str()) else {
            return Err(format!("Could not extract a valid file extension for \"file_path\" property of \"{}\"", file_path).into())
        };
        Ok(match ext {
            "avro" => Self::Avro(serde_json::from_str(&json_string)?),
            "txt" | "csv" => Self::Delimited(serde_json::from_str(&json_string)?),
            "xlsx" | "xls" => Self::Excel(serde_json::from_str(&json_string)?),
            "geojson" => Self::GeoJson(serde_json::from_str(&json_string)?),
            "ipc" | "feather" => Self::Ipc(serde_json::from_str(&json_string)?),
            "parquet" => Self::Parquet(serde_json::from_str(&json_string)?),
            "shp" => Self::Shape(serde_json::from_str(&json_string)?),
            _ => return Err(format!("Could not extract a data loader for the extension, \"{}\"", ext).into())
        })
    }

    fn copy_statement(&self, copy_options: CopyOptions) -> String {
        match self {
            Self::ArcGis(options) => copy_options.copy_statement(options),
            Self::Avro(options) => copy_options.copy_statement(options),
            Self::Delimited(options) => copy_options.copy_statement(options),
            Self::Excel(options) => copy_options.copy_statement(options),
            Self::GeoJson(options) => copy_options.copy_statement(options),
            Self::Ipc(options) => copy_options.copy_statement(options),
            Self::Parquet(options) => copy_options.copy_statement(options),
            Self::Shape(options) => copy_options.copy_statement(options),
        }
    }

    pub async fn schema(&self) -> BulkDataResult<Schema> {
        match self {
            Self::ArcGis(options) => arc_gis_schema(options).await,
            Self::Avro(options) => avro_schema(options),
            Self::Delimited(options) => delimited_schema(options).await,
            Self::Excel(options) => excel_schema(options),
            Self::GeoJson(options) => geo_json_schema(options),
            Self::Ipc(options) => ipc_schema(options),
            Self::Parquet(options) => parquet_schema(options),
            Self::Shape(options) => shape_schema(options),
        }
    }

    async fn spool_records(self, record_channel: &mut RecordSpoolChannel) -> RecordSpoolResult {
        match &self {
            Self::ArcGis(options) => arc_gis_spool_records(options, record_channel).await,
            Self::Avro(options) => avro_spool_records(options, record_channel).await,
            Self::Delimited(options) => delimited_spool_records(options, record_channel).await,
            Self::Excel(options) => excel_spool_records(options, record_channel).await,
            Self::GeoJson(options) => geo_json_spool_records(options, record_channel).await,
            Self::Ipc(options) => ipc_spool_records(options, record_channel).await,
            Self::Parquet(options) => parquet_spool_records(options, record_channel).await,
            Self::Shape(options) => shape_spool_records(options, record_channel).await,
        }
    }

    pub async fn load_data(self, copy_options: CopyOptions, pool: PgPool) -> BulkLoadResult {
        let copy_statement = self.copy_statement(copy_options);
        let mut copy = pool.copy_in_raw(&copy_statement).await?;
        let (mut tx, mut rx) = mpsc_channel(1000);
        let spool_handle = tokio::spawn(async move {
            let error = self.spool_records(&mut tx).await;
            drop(tx);
            error
        });
        let result = loop {
            match rx.recv().await {
                Some(Ok(record)) => {
                    if let Err(error) = copy.send(record.as_bytes()).await {
                        break Err(format!(
                            "Error trying to send record \"{}\".\n{}",
                            record, error
                        )
                        .into());
                    }
                }
                Some(Err(error)) => break Err(error),
                None => break Ok(()),
            }
        };
        rx.close();
        match spool_handle.await {
            Ok(Some(value)) => println!("SendError\n{:?}", value.0),
            Ok(None) => println!("Finished spool handle successfully"),
            Err(error) => println!("Error trying to finish the spool handle\n{}", error),
        }
        match result {
            Ok(_) => Ok(copy.finish().await?),
            Err(error) => {
                copy.abort(format!("{}", error)).await?;
                Err(error)
            }
        }
    }
}
