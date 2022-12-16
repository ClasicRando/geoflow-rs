use rocket::{delete, get, post, put, serde::msgpack::MsgPack, State};
use sqlx::postgres::PgPool;
use workflow_engine::server::MsgPackApiResponse;

use crate::database::{source_data::SourceData, users::User};

#[post("/api/v1/bulk-loading/source-data", data = "<source_data>")]
pub async fn create_source_data(
    source_data: MsgPack<SourceData>,
    pool: &State<PgPool>,
    user: User,
) -> MsgPackApiResponse<SourceData> {
    SourceData::create(source_data.0, user.uid, pool)
        .await
        .into()
}

#[get("/api/v1/bulk-loading/source-data/<sd_id>")]
pub async fn read_single_source_data(
    sd_id: i64,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<SourceData> {
    match SourceData::read_single(sd_id, pool).await {
        Ok(Some(record)) => MsgPackApiResponse::success(record),
        Ok(None) => {
            MsgPackApiResponse::failure(format!("Could not find a record for sd_id = {}", sd_id))
        }
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[get("/api/v1/bulk-loading/source-data/load-instance/<li_id>")]
pub async fn read_many_source_data(
    li_id: i64,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<Vec<SourceData>> {
    SourceData::read_many(li_id, pool).await.into()
}

#[put("/api/v1/bulk-loading/source-data", data = "<source_data>")]
pub async fn update_source_data(
    source_data: MsgPack<SourceData>,
    pool: &State<PgPool>,
    user: User,
) -> MsgPackApiResponse<SourceData> {
    source_data.0.update(user.uid, pool).await.into()
}

#[delete("/api/v1/bulk-loading/source-data/<sd_id>")]
pub async fn delete_source_data(
    sd_id: i64,
    pool: &State<PgPool>,
    user: User,
) -> MsgPackApiResponse<SourceData> {
    match SourceData::delete(sd_id, user.uid, pool).await {
        Ok(Some(record)) => MsgPackApiResponse::success(record),
        Ok(None) => {
            MsgPackApiResponse::failure(format!("Could not find a record for sd_id = {}", sd_id))
        }
        Err(error) => MsgPackApiResponse::error(error),
    }
}
