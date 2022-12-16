use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use crate::bulk_loading::ColumnMetadata;

use super::utilities::start_transaction;

#[derive(sqlx::FromRow, Serialize, Deserialize)]
pub struct SourceData {
    #[serde(default)]
    pub sd_id: i64,
    li_id: i64,
    #[serde(default)]
    pub load_source_id: i16,
    user_generated: bool,
    options: Value,
    table_name: String,
    columns: Vec<ColumnMetadata>,
    to_load: bool,
    loaded_timestamp: Option<chrono::DateTime<Utc>>,
    error_message: Option<String>,
}

impl SourceData {
    pub async fn create(mut data: Self, uid: i64, pool: &PgPool) -> Result<Self, sqlx::Error> {
        let mut transaction = start_transaction(&uid, pool).await?;
        let (sd_id, load_source_id): (i64, i16) =
            sqlx::query_as("select geoflow.create_source_data_entry($1,$2,$3,$4,$5,$6)")
                .bind(uid)
                .bind(data.li_id)
                .bind(data.user_generated)
                .bind(&data.options)
                .bind(&data.table_name)
                .bind(&data.columns)
                .fetch_one(&mut transaction)
                .await?;
        transaction.commit().await?;
        data.sd_id = sd_id;
        data.load_source_id = load_source_id;
        Ok(data)
    }

    pub async fn read_single(sd_id: i64, pool: &PgPool) -> Result<Option<Self>, sqlx::Error> {
        let record: Option<SourceData> = sqlx::query_as("select geoflow.get_source_data_entry($1)")
            .bind(sd_id)
            .fetch_optional(pool)
            .await?;
        Ok(record)
    }

    pub async fn read_many(li_id: i64, pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        let records: Vec<SourceData> = sqlx::query_as("select * from geoflow.get_source_data($1)")
            .bind(li_id)
            .fetch_all(pool)
            .await?;
        Ok(records)
    }

    pub async fn read_many_to_load(
        workflow_run_id: &i64,
        pool: &PgPool,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let records: Vec<SourceData> =
            sqlx::query_as("select * from geoflow.get_source_data_to_load($1)")
                .bind(workflow_run_id)
                .fetch_all(pool)
                .await?;
        Ok(records)
    }

    pub async fn update(self, uid: i64, pool: &PgPool) -> Result<Self, sqlx::Error> {
        let mut transaction = start_transaction(&uid, pool).await?;
        let new_state: SourceData =
            sqlx::query_as("select geoflow.update_source_data_entry($1,$2,$3,$4,$5,$6,$7)")
                .bind(uid)
                .bind(self.sd_id)
                .bind(self.load_source_id)
                .bind(self.user_generated)
                .bind(&self.options)
                .bind(&self.table_name)
                .bind(&self.columns)
                .fetch_one(&mut transaction)
                .await?;
        transaction.commit().await?;
        Ok(new_state)
    }

    pub async fn delete(sd_id: i64, uid: i64, pool: &PgPool) -> Result<Option<Self>, sqlx::Error> {
        let mut transaction = start_transaction(&uid, pool).await?;
        let record = sqlx::query_as("selct geoflow.delete_source_data_entry($1,$2)")
            .bind(uid)
            .bind(sd_id)
            .fetch_optional(&mut transaction)
            .await?;
        transaction.commit().await?;
        Ok(record)
    }
}
