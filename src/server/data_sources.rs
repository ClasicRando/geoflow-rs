use rocket::{delete, get, patch, post, serde::msgpack::MsgPack, State};
use sqlx::postgres::PgPool;
use workflow_engine::server::MsgPackApiResponse;

use crate::database::{
    data_sources::{DataSource, DataSourceContact, DataSourceRequest},
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

#[post(
    "/data-source/<ds_id>/contact",
    format = "msgpack",
    data = "<data_source_contact>"
)]
pub async fn create_data_source_contact(
    ds_id: i64,
    user: User,
    data_source_contact: MsgPack<DataSourceContact>,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<DataSourceContact> {
    if !user.is_collection() {
        return MsgPackApiResponse::failure(String::from(
            "User cannot create data source contacts",
        ));
    }
    match DataSourceContact::create(user.uid, ds_id, data_source_contact.0, pool).await {
        Ok(Some(contact)) => MsgPackApiResponse::success(contact),
        Ok(None) => MsgPackApiResponse::failure(String::from(
            "Data source contact creation was not successful",
        )),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[get("/data-source-contact/<contact_id>")]
pub async fn read_data_source_contact(
    contact_id: i64,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<DataSourceContact> {
    match DataSourceContact::read_one(contact_id, pool).await {
        Ok(Some(contact)) => MsgPackApiResponse::success(contact),
        Ok(None) => MsgPackApiResponse::failure(format!(
            "Could not find a data source contact for contact_id = {}",
            contact_id
        )),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[get("/data-source/<ds_id>/contacts")]
pub async fn read_data_source_contacts(
    ds_id: i64,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<Vec<DataSourceContact>> {
    match DataSourceContact::read_many(ds_id, pool).await {
        Ok(contacts) => MsgPackApiResponse::success(contacts),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[patch(
    "/data-source-contact",
    format = "msgpack",
    data = "<data_source_contact>"
)]
pub async fn update_data_source_contact(
    user: User,
    data_source_contact: MsgPack<DataSourceContact>,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<DataSourceContact> {
    if !user.is_collection() {
        return MsgPackApiResponse::failure(String::from(
            "User cannot update data source contacts",
        ));
    }
    match DataSourceContact::update(user.uid, data_source_contact.0, pool).await {
        Ok(Some(contact)) => MsgPackApiResponse::success(contact),
        Ok(None) => {
            MsgPackApiResponse::failure(String::from("Data source creation was not successful"))
        }
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[delete("/data-source-contact/<contact_id>")]
pub async fn delete_data_source_contact(
    contact_id: i64,
    user: User,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<DataSourceContact> {
    if !user.is_collection() {
        return MsgPackApiResponse::failure(String::from(
            "User cannot delete data source contacts",
        ));
    }
    match DataSourceContact::delete(user.uid, contact_id, pool).await {
        Ok(true) => MsgPackApiResponse::message(format!("Deleted contact_id = {}", contact_id)),
        Ok(false) => MsgPackApiResponse::failure(format!(
            "Could not delete contact for contact_id = {}",
            contact_id
        )),
        Err(error) => MsgPackApiResponse::error(error),
    }
}
