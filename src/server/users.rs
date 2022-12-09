use rocket::{
    get,
    http::{Cookie, CookieJar},
    patch, post,
    serde::msgpack::MsgPack,
    State,
};
use serde::Deserialize;
use sqlx::postgres::PgPool;
use workflow_engine::server::MsgPackApiResponse;

use crate::database::users::User;

#[post("/api/v1/login", data = "<user>")]
pub async fn login(
    user: MsgPack<User>,
    pool: &State<PgPool>,
    cookies: &CookieJar<'_>,
) -> MsgPackApiResponse<User> {
    match user.0.validate_user(pool).await {
        Ok(Some(user)) => {
            cookies.add_private(Cookie::new("x-geoflow-uid", user.uid.to_string()));
            MsgPackApiResponse::success(user)
        }
        Ok(None) => {
            MsgPackApiResponse::failure(String::from("Failed to login. Invalid credentials"))
        }
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[post("/api/v1/users", data = "<user>")]
pub async fn create_user(
    user: MsgPack<User>,
    pool: &State<PgPool>,
    current_user: User,
) -> MsgPackApiResponse<User> {
    if !current_user.is_admin() {
        return MsgPackApiResponse::failure(
            "Current user does not have privileges to create users".to_string(),
        );
    }
    match user.0.create_user(pool).await {
        Ok(Some(user)) => MsgPackApiResponse::success(user),
        Ok(None) => MsgPackApiResponse::failure(String::from("Failed to create a new user")),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[get("/api/v1/users/<uid>")]
pub async fn read_user(uid: i64, user: User) -> MsgPackApiResponse<User> {
    if user.is_admin() || user.uid == uid {
        return MsgPackApiResponse::success(user);
    }
    MsgPackApiResponse::failure(format!(
        "Current user does not have privileges to view uid = {}",
        uid
    ))
}

#[get("/api/v1/users")]
pub async fn read_users(user: User, pool: &State<PgPool>) -> MsgPackApiResponse<Vec<User>> {
    if !user.is_admin() {
        return MsgPackApiResponse::failure(
            "Current user does not have privileges to view users".to_string(),
        );
    }
    User::read_many(pool).await.into()
}

#[derive(Deserialize)]
pub struct UpdatePassword {
    old_password: String,
    new_password: String,
}

#[patch("/api/v1/users/update-password", data = "<update_password>")]
pub async fn update_user_password(
    update_password: MsgPack<UpdatePassword>,
    user: User,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<User> {
    let UpdatePassword {
        old_password,
        new_password,
    } = update_password.0;
    match User::update_password(user.username, old_password, new_password, pool).await {
        Ok(Some(user)) => MsgPackApiResponse::success(user),
        Ok(None) => MsgPackApiResponse::failure(String::from("Failed to update the user password")),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[derive(Deserialize)]
pub struct UpdateName(String);

#[patch("/api/v1/users/update-name", data = "<update_name>")]
pub async fn update_user_name(
    update_name: MsgPack<UpdateName>,
    user: User,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<User> {
    match User::update_name(user.uid, update_name.0 .0, pool).await {
        Ok(Some(user)) => MsgPackApiResponse::success(user),
        Ok(None) => MsgPackApiResponse::failure(String::from("Failed to update the user's name")),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

