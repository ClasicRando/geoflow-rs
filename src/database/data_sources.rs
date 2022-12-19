use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{
    decode::Decode,
    encode::{Encode, IsNull},
    postgres::{
        types::{PgRecordDecoder, PgRecordEncoder},
        PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef,
    },
    PgPool, Postgres, Type,
};

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct Region {
    region_id: i64,
    country_code: String,
    country_name: String,
    prov_code: Option<String>,
    prov_name: Option<String>,
    county: Option<String>,
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct WarehouseType {
    wt_id: i32,
    #[sqlx(rename = "warehouse_name")]
    name: String,
    #[sqlx(rename = "warehouse_description")]
    description: String,
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct DataSourceContact {
    contact_id: i64,
    name: String,
    email: Option<String>,
    website: Option<String>,
    contact_type: Option<String>,
    notes: Option<String>,
    #[serde(default)]
    created: NaiveDateTime,
    #[serde(default)]
    created_by: String,
    #[serde(default)]
    last_updated: Option<NaiveDateTime>,
    #[serde(default)]
    updated_by: Option<String>,
}

impl Encode<'_, Postgres> for DataSourceContact {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
        let mut encoder = PgRecordEncoder::new(buf);
        encoder.encode(self.contact_id);
        encoder.encode(&self.name);
        encoder.encode(&self.email);
        encoder.encode(&self.website);
        encoder.encode(&self.contact_type);
        encoder.encode(&self.notes);
        encoder.encode(self.created);
        encoder.encode(&self.created_by);
        encoder.encode(self.last_updated);
        encoder.encode(&self.updated_by);
        encoder.finish();
        IsNull::No
    }

    fn size_hint(&self) -> ::std::primitive::usize {
        10usize * (4 + 4)
            + <i64 as Encode<Postgres>>::size_hint(&self.contact_id)
            + <String as Encode<Postgres>>::size_hint(&self.name)
            + <Option<String> as Encode<Postgres>>::size_hint(&self.email)
            + <Option<String> as Encode<Postgres>>::size_hint(&self.website)
            + <Option<String> as Encode<Postgres>>::size_hint(&self.contact_type)
            + <Option<String> as Encode<Postgres>>::size_hint(&self.notes)
            + <NaiveDateTime as Encode<Postgres>>::size_hint(&self.created)
            + <String as Encode<Postgres>>::size_hint(&self.created_by)
            + <Option<NaiveDateTime> as Encode<Postgres>>::size_hint(&self.last_updated)
            + <Option<String> as Encode<Postgres>>::size_hint(&self.updated_by)
    }
}

impl<'r> Decode<'r, Postgres> for DataSourceContact {
    fn decode(
        value: PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let mut decoder = PgRecordDecoder::new(value)?;
        let contact_id = decoder.try_decode::<i64>()?;
        let name = decoder.try_decode::<String>()?;
        let email = decoder.try_decode::<Option<String>>()?;
        let website = decoder.try_decode::<Option<String>>()?;
        let contact_type = decoder.try_decode::<Option<String>>()?;
        let notes = decoder.try_decode::<Option<String>>()?;
        let created = decoder.try_decode::<NaiveDateTime>()?;
        let created_by = decoder.try_decode::<String>()?;
        let last_updated = decoder.try_decode::<Option<NaiveDateTime>>()?;
        let updated_by = decoder.try_decode::<Option<String>>()?;
        Ok(DataSourceContact {
            contact_id,
            name,
            email,
            website,
            contact_type,
            notes,
            created,
            created_by,
            last_updated,
            updated_by,
        })
    }
}

impl Type<Postgres> for DataSourceContact {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("data_source_contact")
    }
}

impl PgHasArrayType for DataSourceContact {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_data_source_contact")
    }
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct DataSource {
    ds_id: i64,
    name: String,
    description: String,
    search_radius: f32,
    comments: Option<String>,
    #[sqlx(flatten)]
    region: Region,
    assigned_user: String,
    created: NaiveDateTime,
    created_by: String,
    last_updated: NaiveDateTime,
    updated_by: String,
    #[sqlx(flatten)]
    warehouse_type: WarehouseType,
    collection_workflow: i64,
    load_workflow: i64,
    check_workflow: i64,
    contacts: Vec<DataSourceContact>,
}

#[derive(Deserialize)]
pub struct DataSourceRequest {
    #[serde(default)]
    ds_id: i64,
    name: String,
    description: String,
    search_radius: f32,
    #[serde(default)]
    comments: Option<String>,
    region_id: i64,
    warehouse_type_id: i32,
    collection_workflow: i64,
    load_workflow: i64,
    check_workflow: i64,
}

impl DataSource {
    pub async fn create(
        uid: i64,
        request: DataSourceRequest,
        pool: &PgPool,
    ) -> Result<Option<Self>, sqlx::Error> {
        let result =
            sqlx::query_scalar("select geoflow.init_data_source($1,$2,$3,$4,$5,$6,$7,$8,$9)")
                .bind(uid)
                .bind(&request.name)
                .bind(&request.description)
                .bind(request.search_radius)
                .bind(request.region_id)
                .bind(request.warehouse_type_id)
                .bind(request.collection_workflow)
                .bind(request.load_workflow)
                .bind(request.check_workflow)
                .fetch_optional(pool)
                .await?;
        let Some(ds_id) = result else {
            return Ok(None)
        };
        Self::read_one(ds_id, pool).await
    }

    pub async fn read_one(ds_id: i64, pool: &PgPool) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as(
            r#"
            select ds_id, name, description, search_radius, comments,
                   region_id, country_code, country_name, prov_code, prov_name, county,
                   assigned_user, created, created_by, last_updated, updated_by,
                   wt_id, warehouse_name, warehouse_description,
                   collection_workflow, load_workflow, check_workflow, contacts
            from   geoflow.v_data_sources
            where  ds_id = $1"#,
        )
        .bind(ds_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn read_many(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as(
            r#"
            select ds_id, name, description, search_radius, comments,
                   region_id, country_code, country_name, prov_code, prov_name, county,
                   assigned_user, created, created_by, last_updated, updated_by,
                   wt_id, warehouse_name, warehouse_description,
                   collection_workflow, load_workflow, check_workflow, contacts
            from   geoflow.v_data_sources"#,
        )
        .fetch_all(pool)
        .await
    }

    pub async fn update(
        uid: i64,
        request: DataSourceRequest,
        pool: &PgPool,
    ) -> Result<Option<Self>, sqlx::Error> {
        if request.ds_id == 0 {
            return Ok(None)
        }
        sqlx::query("call geoflow.update_data_source($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
            .bind(uid)
            .bind(request.ds_id)
            .bind(&request.name)
            .bind(&request.description)
            .bind(request.search_radius)
            .bind(&request.comments)
            .bind(request.region_id)
            .bind(request.warehouse_type_id)
            .bind(request.collection_workflow)
            .bind(request.load_workflow)
            .bind(request.check_workflow)
            .execute(pool)
            .await?;
        Self::read_one(request.ds_id, pool).await
    }
}
