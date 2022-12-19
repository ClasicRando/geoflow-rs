use async_once_cell::OnceCell;
use sqlx::{
    error::Error,
    postgres::{PgConnectOptions, PgPool, PgPoolOptions},
    Postgres, Transaction,
};
use std::env;

static GF_POSTGRES_DB: OnceCell<PgPool> = OnceCell::new();

pub fn db_options() -> PgConnectOptions {
    let we_host_address = env!("GF_HOST");
    let we_db_name = env!("GF_DB");
    let we_db_user = env!("GF_USER");
    let we_db_password = env!("GF_PASSWORD");
    PgConnectOptions::new()
        .host(we_host_address)
        .database(we_db_name)
        .username(we_db_user)
        .password(we_db_password)
        .options([("search_path", "geoflow")])
}

pub async fn create_db_pool() -> Result<PgPool, Error> {
    let options = db_options();
    let pool = PgPoolOptions::new()
        .min_connections(10)
        .max_connections(20)
        .connect_with(options)
        .await?;
    Ok(pool)
}

pub async fn db_pool() -> Result<&'static PgPool, Error> {
    GF_POSTGRES_DB.get_or_try_init(create_db_pool()).await
}

pub async fn start_transaction<'p>(
    geoflow_user_id: &i64,
    pool: &'p PgPool,
) -> Result<Transaction<'p, Postgres>, sqlx::Error> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET LOCAL geoflow.uid = $1")
        .bind(geoflow_user_id)
        .execute(&mut transaction)
        .await?;
    Ok(transaction)
}
