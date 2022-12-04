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
    pub async fn create(mut data: Self, pool: &PgPool) -> Result<Self, sqlx::Error> {
        let (sd_id, load_source_id): (i64, i16) = sqlx::query_as(
            r#"
            insert into geoflow.source_data(li_id,user_generated,options,table_name,columns)
            values($1,$2,$3,$4,$5)
            returning sd_id, load_source_id"#,
        )
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
        let record: Option<SourceData> = sqlx::query_as(
            r#"
            select sd_id, li_id, load_source_id, user_generated, options, table_name, columns
            from   source_data
            where  sd_id = $1"#,
        )
        .bind(sd_id)
        .fetch_optional(pool)
        .await?;
        Ok(record)
    }

    pub async fn read_many(li_id: i64, pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        let records: Vec<SourceData> = sqlx::query_as(
            r#"
            select sd_id, li_id, load_source_id, user_generated, options, table_name, columns
            from   source_data
            where  li_id = $1"#,
        )
        .bind(li_id)
        .fetch_all(pool)
        .await?;
        Ok(records)
    }

    pub async fn update(self, pool: &PgPool) -> Result<Self, sqlx::Error> {
        let new_state: SourceData = sqlx::query_as(
            r#"
            update geoflow.source_data
            set    load_source_id = $1,
                   user_generated = $2,
                   options = $3,
                   table_name = $4,
                   columns = $5
            where  sd_id = $6
            returning sd_id, li_id, load_source_id, user_generated, options, table_name, columns"#,
        )
        .bind(self.load_source_id)
        .bind(self.user_generated)
        .bind(&self.options)
        .bind(&self.table_name)
        .bind(&self.columns)
        .bind(self.sd_id)
        .fetch_one(pool)
        .await?;
        Ok(new_state)
    }

    pub async fn delete(sd_id: i64, pool: &PgPool) -> Result<Option<Self>, sqlx::Error> {
        let record = sqlx::query_as(
            r#"
            delete from geoflow.source_data
            where  sd_id = $1
            returning sd_id, li_id, load_source_id, user_generated, options, table_name, columns"#,
        )
        .bind(sd_id)
        .fetch_optional(pool)
        .await?;
        Ok(record)
    }
}
