mod bulk_loading;
mod users;

use crate::database::utilities::create_db_pool;
use bulk_loading::{
    create_source_data, delete_source_data, read_many_source_data, read_single_source_data,
    update_source_data,
};
use rocket::{routes, Build, Rocket};
use users::{
    add_user_role, create_user, login, read_user, read_users, remove_user_role, update_user_name,
    update_user_password,
};

pub async fn build_server() -> Result<Rocket<Build>, sqlx::Error> {
    let pool = create_db_pool().await?;
    Ok(rocket::build().manage(pool).mount(
        "/api/v1/",
        routes![
            create_source_data,
            read_single_source_data,
            read_many_source_data,
            update_source_data,
            delete_source_data,
            login,
            create_user,
            read_user,
            read_users,
            update_user_password,
            update_user_name,
            add_user_role,
            remove_user_role,
        ],
    ))
}
