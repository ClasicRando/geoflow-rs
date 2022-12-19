use rocket::{get, patch, post, serde::msgpack::MsgPack, State};
use sqlx::postgres::PgPool;
use workflow_engine::server::MsgPackApiResponse;

use crate::database::{
    data_sources::{DataSource, DataSourceRequest},
    users::User,
};

#[post("/data-sources", format = "msgpack", data = "<data_source_request>")]
pub async fn create_data_source(
    user: User,
    data_source_request: MsgPack<DataSourceRequest>,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<DataSource> {
    if !user.can_create_data_source() {
        return MsgPackApiResponse::failure(String::from("User cannot create data sources"));
    }
    match DataSource::create(user.uid, data_source_request.0, pool).await {
        Ok(Some(ds)) => MsgPackApiResponse::success(ds),
        Ok(None) => {
            MsgPackApiResponse::failure(String::from("Data source creation was not successful"))
        }
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[get("/data-sources/<ds_id>")]
pub async fn read_data_source(ds_id: i64, pool: &State<PgPool>) -> MsgPackApiResponse<DataSource> {
    match DataSource::read_one(ds_id, pool).await {
        Ok(Some(ds)) => MsgPackApiResponse::success(ds),
        Ok(None) => MsgPackApiResponse::failure(format!(
            "Could not find a data source for ds_id = {}",
            ds_id
        )),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[get("/data-sources")]
pub async fn read_data_sources(pool: &State<PgPool>) -> MsgPackApiResponse<Vec<DataSource>> {
    match DataSource::read_many(pool).await {
        Ok(sources) => MsgPackApiResponse::success(sources),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[patch("/data-sources", format = "msgpack", data = "<data_source_request>")]
pub async fn update_data_source(
    user: User,
    data_source_request: MsgPack<DataSourceRequest>,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<DataSource> {
    if !user.is_collection() {
        return MsgPackApiResponse::failure(String::from("User cannot update data sources"));
    }
    match DataSource::update(user.uid, data_source_request.0, pool).await {
        Ok(Some(ds)) => MsgPackApiResponse::success(ds),
        Ok(None) => {
            MsgPackApiResponse::failure(String::from("Data source creation was not successful"))
        }
        Err(error) => MsgPackApiResponse::error(error),
    }
}
