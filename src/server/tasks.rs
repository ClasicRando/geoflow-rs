use rocket::{post, serde::msgpack::MsgPack, State};
use sqlx::postgres::PgPool;
use workflow_engine::{ApiReponse as WEApiResponse, TaskQueueRecord};

use crate::tasks::bulk_load::task_run_bulk_load;

#[post("/task/run/bulk-load", data = "<task_queue_record>")]
pub async fn run_bulk_load(
    task_queue_record: MsgPack<TaskQueueRecord>,
    pool: &State<PgPool>,
) -> MsgPack<WEApiResponse> {
    let response = task_run_bulk_load(task_queue_record.0, pool).await;
    MsgPack(response)
}
