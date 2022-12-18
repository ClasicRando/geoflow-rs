use rocket::{
    delete, get,
    http::{Cookie, CookieJar},
    patch, post,
    serde::msgpack::MsgPack,
    time::{Duration, OffsetDateTime},
    State,
};
use serde::Deserialize;
use sqlx::postgres::PgPool;
use workflow_engine::server::MsgPackApiResponse;

use crate::database::users::User;

#[post("/login", format = "msgpack", data = "<user>")]
pub async fn login(
    user: MsgPack<User>,
    pool: &State<PgPool>,
    cookies: &CookieJar<'_>,
) -> MsgPackApiResponse<User> {
    match user.0.validate_user(pool).await {
        Ok(Some(user)) => {
            let mut now = OffsetDateTime::now_utc();
            now += Duration::days(1);
            let cookie = Cookie::build("x-geoflow-uid", user.uid.to_string())
                .expires(now)
                .finish();
            cookies.add_private(cookie);
            MsgPackApiResponse::success(user)
        }
        Ok(None) => {
            MsgPackApiResponse::failure(String::from("Failed to login. Invalid credentials"))
        }
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[post("/logout")]
pub async fn logout(cookies: &CookieJar<'_>) -> MsgPackApiResponse<&'static str> {
    cookies.remove_private(Cookie::named("x-geoflow-uid"));
    MsgPackApiResponse::success("Successfuly logged out user")
}

#[post("/users", format = "msgpack", data = "<user>")]
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
    match user.0.create_user(current_user.uid, pool).await {
        Ok(Some(user)) => MsgPackApiResponse::success(user),
        Ok(None) => MsgPackApiResponse::failure(String::from("Failed to create a new user")),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[get("/users/<uid>")]
pub async fn read_user(uid: i64, user: User) -> MsgPackApiResponse<User> {
    if user.is_admin() || user.uid == uid {
        return MsgPackApiResponse::success(user);
    }
    MsgPackApiResponse::failure(format!(
        "Current user does not have privileges to view uid = {}",
        uid
    ))
}

#[get("/users")]
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

#[patch("/users/update-password", format = "msgpack", data = "<update_password>")]
pub async fn update_user_password(
    update_password: MsgPack<UpdatePassword>,
    user: User,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<User> {
    let UpdatePassword {
        old_password,
        new_password,
    } = update_password.0;
    match User::update_password(user.uid, user.username, old_password, new_password, pool).await {
        Ok(Some(user)) => MsgPackApiResponse::success(user),
        Ok(None) => MsgPackApiResponse::failure(String::from("Failed to update the user password")),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[derive(Deserialize)]
pub struct UpdateName(String);

#[patch("/users/update-name", format = "msgpack", data = "<update_name>")]
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

#[derive(Deserialize)]
pub struct AlterRole {
    uid: i64,
    role_id: i32,
}

#[post("/users/roles", format = "msgpack", data = "<add_role>")]
pub async fn add_user_role(
    add_role: MsgPack<AlterRole>,
    user: User,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<User> {
    if !user.is_admin() {
        return MsgPackApiResponse::failure(
            "Current user does not have privileges to add roles".to_string(),
        );
    }
    match User::add_role(user.uid, add_role.uid, add_role.role_id, pool).await {
        Ok(Some(user)) => MsgPackApiResponse::success(user),
        Ok(None) => MsgPackApiResponse::failure(format!(
            "Failed to add role_id = {} for uid = {}",
            add_role.uid, add_role.role_id
        )),
        Err(error) => MsgPackApiResponse::error(error),
    }
}

#[delete("/users/roles", format = "msgpack", data = "<remove_role>")]
pub async fn remove_user_role(
    remove_role: MsgPack<AlterRole>,
    user: User,
    pool: &State<PgPool>,
) -> MsgPackApiResponse<User> {
    if !user.is_admin() {
        return MsgPackApiResponse::failure(
            "Current user does not have privileges to add roles".to_string(),
        );
    }
    match User::remove_role(user.uid, remove_role.uid, remove_role.role_id, pool).await {
        Ok(Some(user)) => MsgPackApiResponse::success(user),
        Ok(None) => MsgPackApiResponse::failure(format!(
            "Failed to remove role_id = {} for uid = {}",
            remove_role.uid, remove_role.role_id
        )),
        Err(error) => MsgPackApiResponse::error(error),
    }
}
