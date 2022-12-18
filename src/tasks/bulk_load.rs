use sqlx::postgres::PgPool;
use workflow_engine::{ApiReponse as WEApiResponse, TaskQueueRecord};

use crate::{
    bulk_loading::{error::BulkDataResult, DataLoader},
    database::source_data::SourceData,
};

const DB_SCHEMA: &str = "bulk_loading";

async fn load_source_data(source_data: &SourceData, pool: &PgPool) -> BulkDataResult<u64> {
    let loader = DataLoader::new(&source_data.options)?;
    let schema = loader.schema().await?;

    sqlx::query(&format!(
        "drop table if exists {}.{}",
        DB_SCHEMA,
        schema.table_name()
    ))
    .execute(pool)
    .await?;
    let create_statement = schema.create_statement(DB_SCHEMA);
    sqlx::query(&create_statement).execute(pool).await?;

    let copy_options = schema.copy_options(DB_SCHEMA);
    loader.load_data(copy_options, pool).await
}

/// Task to execute a bulk load operation
pub async fn task_run_bulk_load(
    task_queue_record: TaskQueueRecord,
    pool: &PgPool,
) -> WEApiResponse {
    let workflow_run_id = &task_queue_record.workflow_run_id;
    let source_data_to_load = match SourceData::read_many_to_load(workflow_run_id, pool).await {
        Ok(inner) => inner,
        Err(error) => {
            return WEApiResponse::new(
                400,
                false,
                Some(format!("SQL Error fetching source data to load {}", error)),
                None,
            )
        }
    };
    let mut errors = Vec::new();
    let mut results = Vec::new();
    for source_data in source_data_to_load {
        match load_source_data(&source_data, pool).await {
            Ok(count) => results.push((source_data.sd_id, count)),
            Err(error) => {
                errors.push(format!(
                    "Error attempting to bulk load data for sd_id = {}.\n{}",
                    source_data.sd_id, error
                ));
            }
        }
    }
    if errors.is_empty() {
        WEApiResponse::new(200, true, Some(format!("Results: {:?}", results)), None)
    } else {
        WEApiResponse::new(
            400,
            false,
            Some(format!("Results: {:?}\nErrors: {:?}", results, errors)),
            None,
        )
    }
}
