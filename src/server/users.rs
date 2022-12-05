use rocket::{
    delete, get,
    http::{Cookie, CookieJar},
    post, put,
    serde::msgpack::MsgPack,
    State,
};
use sqlx::postgres::PgPool;
use workflow_engine::ApiResponse;

use crate::database::users::User;

#[post("/api/v1/login", data = "<user>")]
pub async fn login(
    user: MsgPack<User>,
    pool: &State<PgPool>,
    cookies: &CookieJar<'_>,
) -> ApiResponse<User> {
    match user.0.validate_user(pool).await {
        Ok(Some(user)) => {
            cookies.add_private(Cookie::new("x-geoflow-uid", user.uid.to_string()));
            ApiResponse::success(user)
        }
        Ok(None) => ApiResponse::failure(400, String::from("Failed to login. Invalid credentials")),
        Err(error) => ApiResponse::failure_with_error(error),
    }
}

#[post("/api/v1/users/<uid>")]
pub async fn read_user(
    uid: i64,
    user: User,
) -> ApiResponse<User> {
    if user.is_admin() || user.uid == uid {
        return ApiResponse::success(user)
    }
    ApiResponse::failure(400, format!("Current user does not have privileges to view uid = {}", uid))
}