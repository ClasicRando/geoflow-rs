use sqlx::{
    error::Error,
    postgres::{PgConnectOptions, PgPool, PgPoolOptions},
};
use std::env;

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
