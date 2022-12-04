use rocket::{delete, get, post, put, serde::msgpack::MsgPack, State};
use sqlx::postgres::PgPool;
use workflow_engine::ApiResponse;

use crate::database::source_data::SourceData;

#[post("/bulk-loading/source-data", data = "<source_data>")]
pub async fn create_source_data(
    source_data: MsgPack<SourceData>,
    pool: &State<PgPool>,
) -> ApiResponse<SourceData> {
    match SourceData::create(source_data.0, pool).await {
        Ok(source_data) => ApiResponse::success(source_data),
        Err(error) => ApiResponse::failure_with_error(error),
    }
}

#[get("/bulk-loading/source-data/<sd_id>")]
pub async fn read_single_source_data(sd_id: i64, pool: &State<PgPool>) -> ApiResponse<SourceData> {
    match SourceData::read_single(sd_id, pool).await {
        Ok(Some(record)) => ApiResponse::success(record),
        Ok(None) => ApiResponse::failure(
            400,
            format!("Could not find a record for sd_id = {}", sd_id),
        ),
        Err(error) => ApiResponse::failure_with_error(error),
    }
}

#[get("/bulk-loading/source-data/load-instance/<li_id>")]
pub async fn read_many_source_data(
    li_id: i64,
    pool: &State<PgPool>,
) -> ApiResponse<Vec<SourceData>> {
    match SourceData::read_many(li_id, pool).await {
        Ok(records) => ApiResponse::success(records),
        Err(error) => ApiResponse::failure_with_error(error),
    }
}

#[put("/bulk-loading/source-data", data = "<source_data>")]
pub async fn update_source_data(
    source_data: MsgPack<SourceData>,
    pool: &State<PgPool>,
) -> ApiResponse<SourceData> {
    match source_data.0.update(pool).await {
        Ok(new_state) => ApiResponse::success(new_state),
        Err(error) => ApiResponse::failure_with_error(error),
    }
}

#[delete("/bulk-loading/source-data/<sd_id>")]
pub async fn delete_source_data(sd_id: i64, pool: &State<PgPool>) -> ApiResponse<SourceData> {
    match SourceData::delete(sd_id, pool).await {
        Ok(Some(record)) => ApiResponse::success(record),
        Ok(None) => ApiResponse::failure(
            400,
            format!("Could not find a record for sd_id = {}", sd_id),
        ),
        Err(error) => ApiResponse::failure_with_error(error),
    }
}
