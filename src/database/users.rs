use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::{PgHasArrayType, PgTypeInfo},
    PgPool,
};

#[derive(sqlx::FromRow, Serialize, Deserialize)]
pub struct User {
    #[serde(default)]
    uid: i64,
    #[serde(default)]
    name: String,
    username: String,
    #[serde(skip_serializing)]
    #[sqlx(default)]
    password: String,
    #[serde(default)]
    roles: Vec<UserRole>,
}

#[derive(sqlx::Type, Serialize, Deserialize)]
#[sqlx(type_name = "roles")]
pub struct UserRole {
    role_id: i32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
}

impl PgHasArrayType for UserRole {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_roles")
    }
}

impl User {
    pub async fn create_user(self, pool: &PgPool) -> Result<Option<Self>, sqlx::Error> {
        let role_ids = self.roles.iter().map(|r| r.role_id).collect::<Vec<_>>();
        let uid_option: Option<i64> = sqlx::query_scalar("select geoflow.create_user($1,$2,$3,$4)")
            .bind(&self.name)
            .bind(&self.username)
            .bind(&self.password)
            .bind(&role_ids)
            .fetch_optional(pool)
            .await?;
        let Some(uid) = uid_option else {
            return Ok(None)
        };
        Self::read_one(uid, pool).await
    }

    pub async fn validate_user(&self, pool: &PgPool) -> Result<bool, sqlx::Error> {
        sqlx::query_scalar("select geoflow.validate_user($1,$2)")
            .bind(&self.username)
            .bind(&self.password)
            .fetch_one(pool)
            .await
    }

    pub async fn read_one(uid: i64, pool: &PgPool) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as(
            r#"
            select uid, name, username, roles
            from   geoflow.v_users
            where  uid = $1"#,
        )
        .bind(uid)
        .fetch_optional(pool)
        .await
    }

    pub async fn read_many(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as(
            r#"
            select uid, name, username, roles
            from   geoflow.v_users"#,
        )
        .fetch_all(pool)
        .await
    }

    pub async fn update_password(
        username: String,
        old_password: String,
        new_password: String,
        pool: &PgPool,
    ) -> Result<Option<Self>, sqlx::Error> {
        let uid: i64 = sqlx::query_scalar("select geoflow.update_user_password($1,$2,$3)")
            .bind(&username)
            .bind(&old_password)
            .bind(&new_password)
            .fetch_one(pool)
            .await?;
        Self::read_one(uid, pool).await
    }

    pub async fn update_name(
        uid: i64,
        name: String,
        pool: &PgPool,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query(
            r#"
            update geoflow.users
            set    name = $2
            where  uid = $1;"#,
        )
        .bind(uid)
        .bind(&name)
        .execute(pool)
        .await?;
        Self::read_one(uid, pool).await
    }

    pub async fn add_role(
        uid: i64,
        role_id: i64,
        pool: &PgPool,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query(
            r#"
            insert into geoflow.user_roles(uid,role_id)
            values($1,$2)"#,
        )
        .bind(uid)
        .bind(role_id)
        .execute(pool)
        .await?;
        Self::read_one(uid, pool).await
    }

    pub async fn remove_role(
        uid: i64,
        role_id: i64,
        pool: &PgPool,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query(
            r#"
            delete from geoflow.user_roles
            where  uid = $1
            and    role_id = $2;"#,
        )
        .bind(uid)
        .bind(role_id)
        .execute(pool)
        .await?;
        Self::read_one(uid, pool).await
    }
}
