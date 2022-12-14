mod bulk_loading;
mod data_sources;
mod tasks;
mod users;

use crate::database::utilities::create_db_pool;
use bulk_loading::{
    create_source_data, delete_source_data, read_many_source_data, read_single_source_data,
    update_source_data,
};
use data_sources::{
    create_data_source, create_data_source_contact, delete_data_source_contact, read_data_source,
    read_data_source_contact, read_data_source_contacts, read_data_sources, update_data_source,
    update_data_source_contact,
};
use rocket::{routes, Build, Config, Rocket};
use tasks::run_bulk_load;
use users::{
    add_user_role, create_user, login, logout, read_user, read_users, remove_user_role,
    update_user_name, update_user_password,
};

pub async fn build_server() -> Result<Rocket<Build>, sqlx::Error> {
    let pool = create_db_pool().await?;
    let config = Config {
        port: 8001,
        ..Default::default()
    };
    Ok(rocket::build().manage(pool).configure(config).mount(
        "/api/v1/",
        routes![
            create_source_data,
            read_single_source_data,
            read_many_source_data,
            update_source_data,
            delete_source_data,
            login,
            logout,
            create_user,
            read_user,
            read_users,
            update_user_password,
            update_user_name,
            add_user_role,
            remove_user_role,
            run_bulk_load,
            create_data_source,
            read_data_source,
            read_data_sources,
            update_data_source,
            create_data_source_contact,
            delete_data_source_contact,
            read_data_source_contact,
            read_data_source_contacts,
            update_data_source_contact,
        ],
    ))
}
