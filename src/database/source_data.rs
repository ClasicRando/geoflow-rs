use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use crate::bulk_loading::analyze::ColumnMetadata;

#[derive(sqlx::FromRow, Serialize, Deserialize)]
pub struct SourceData {
    #[serde(default)]
    sd_id: i64,
    li_id: i64,
    #[serde(default)]
    load_source_id: i16,
    user_generated: bool,
    options: Value,
    table_name: String,
    columns: Vec<ColumnMetadata>,
}

impl SourceData {
    pub async fn create(mut data: Self, uid: i64, pool: &PgPool) -> Result<Self, sqlx::Error> {
        let (sd_id, load_source_id): (i64, i16) =
            sqlx::query_as("select geoflow.create_source_data_entry($1,$2,$3,$4,$5,$6)")
                .bind(uid)
                .bind(data.li_id)
                .bind(data.user_generated)
                .bind(&data.options)
                .bind(&data.table_name)
                .bind(&data.columns)
                .fetch_one(pool)
                .await?;
        data.sd_id = sd_id;
        data.load_source_id = load_source_id;
        Ok(data)
    }

    pub async fn read_single(sd_id: i64, pool: &PgPool) -> Result<Option<Self>, sqlx::Error> {
        let record: Option<SourceData> = sqlx::query_as("select geoflow.get_source_data($1)")
            .bind(sd_id)
            .fetch_optional(pool)
            .await?;
        Ok(record)
    }

    pub async fn read_many(li_id: i64, pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        let records: Vec<SourceData> =
            sqlx::query_as("select * from geoflow.get_source_data_entry($1)")
                .bind(li_id)
                .fetch_all(pool)
                .await?;
        Ok(records)
    }

    pub async fn update(self, uid: i64, pool: &PgPool) -> Result<Self, sqlx::Error> {
        let new_state: SourceData =
            sqlx::query_as("select geoflow.update_source_data_entry($1,$2,$3,$4,$5,$6,$7)")
                .bind(uid)
                .bind(self.sd_id)
                .bind(self.load_source_id)
                .bind(self.user_generated)
                .bind(&self.options)
                .bind(&self.table_name)
                .bind(&self.columns)
                .fetch_one(pool)
                .await?;
        Ok(new_state)
    }

    pub async fn delete(sd_id: i64, uid: i64, pool: &PgPool) -> Result<Option<Self>, sqlx::Error> {
        let record = sqlx::query_as("selct geoflow.delete_source_data_entry($1,$2)")
            .bind(uid)
            .bind(sd_id)
            .fetch_optional(pool)
            .await?;
        Ok(record)
    }
}
