use geoflow_rs::{
    bulk_loading::{DataLoader, ColumnType},
    database::utilities::create_db_pool,
};
use serde_json::json;

const DB_SCHEMA: &str = "bulk_loading";

#[tokio::test]
async fn delimited_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    let expected_table_name = "delimited_data_test";
    let expected_column_names = [
        "facility_id",
        "facility_name",
        "address1",
        "address2",
        "city",
        "state",
        "zip",
        "contact",
        "contact_address1",
        "contact_address2",
        "contact_city",
        "contact_state",
        "contact_zip",
        "tank_id",
        "compartment_tank",
        "manifold_tank",
        "main_tank",
        "root_tank_id",
        "installation_date",
        "perm_close_date",
        "capacity",
        "commercial",
        "regulated",
        "product_name",
        "overfill_protection_name",
        "spill_protection_name",
        "leak_detection_name",
        "tank_constr_name",
        "piping_constr_name",
        "piping_system_name",
        "other_cp_tank",
        "other_cp_name",
        "tank_status_name",
        "fips_county_desc",
        "latitude",
        "longitude",
        "tankcertno",
        "fr_bus_name",
        "fr_amt",
        "fr_desc",
        "last_update_date",
        "certno",
    ];

    let loader = DataLoader::new(&json!({
        "file_path": "tests/delimited data test.csv",
        "delimiter": ",",
        "qualified": true,
    }))?;
    let schema = loader.schema().await?;

    assert_eq!(expected_table_name, schema.table_name());

    let fields = schema.columns();
    assert_eq!(expected_column_names.len(), fields.len());
    for (ex_field, field) in expected_column_names.iter().zip(fields) {
        assert_eq!(*ex_field, field.name());
        assert_eq!(&ColumnType::Text, field.column_type());
    }

    let pool = create_db_pool().await?;
    sqlx::query(&format!(
        "drop table if exists {}.{}",
        DB_SCHEMA,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(DB_SCHEMA);
    sqlx::query(&create_statement).execute(&pool).await?;
    
    let copy_options = schema.copy_options(DB_SCHEMA);
    let records_loaded = loader.load_data(copy_options, &pool).await?;

    assert_eq!(299_u64, records_loaded);

    Ok(())
}

#[tokio::test]
async fn excel_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    let expected_table_name = "excel_data_test";
    let expected_column_names = [
        "incidentnumber",
        "ustnum",
        "incidentname",
        "facilid",
        "address",
        "citytown",
        "state",
        "county",
        "zipcode",
        "mgr",
        "rocode",
        "dateoccurred",
        "rp_company",
        "contact",
        "rpaddress",
        "rpcity",
        "rpstate",
        "rpzipcode",
        "rp_county",
        "source",
        "ptype",
        "datereported",
        "comm",
        "reg",
        "norrissued",
        "novissued",
        "phasereqrd",
        "sitepriority",
        "risk",
        "confrisk",
        "intercons",
        "landuse",
        "typecap",
        "rbca",
        "closreqsd",
        "closeout",
        "contamination",
        "supplywell",
        "mtbe",
        "comment",
        "telephone",
        "flag",
        "errorflag",
        "mtbe1",
        "flag1",
        "releasecode",
        "lurfiled",
        "lur_resc",
        "lur_state",
        "gpsconf",
        "cleanup",
        "currstatus",
        "rbca_gw",
        "petopt",
        "cdnum",
        "reelnum",
        "rpow",
        "rpop",
        "rpl",
        "latdec",
        "longdec",
        "errcd",
        "valid",
        "catcode",
        "hcs_res",
        "hcs_ref",
        "reliability",
        "rp_email",
        "rp_email1",
        "lur_status",
        "newsource",
        "book",
        "page",
    ];

    let loader = DataLoader::new(&json!({
        "file_path": "tests/excel data test.xlsx",
        "sheet_name": "tblUST_DB",
    }))?;
    let schema = loader.schema().await?;

    assert_eq!(expected_table_name, schema.table_name());

    let fields = schema.columns();
    assert_eq!(expected_column_names.len(), fields.len());
    for (ex_field, field) in expected_column_names.iter().zip(fields) {
        assert_eq!(*ex_field, field.name());
        assert_eq!(&ColumnType::Text, field.column_type());
    }

    let pool = create_db_pool().await?;
    sqlx::query(&format!(
        "drop table if exists {}.{}",
        DB_SCHEMA,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(DB_SCHEMA);
    sqlx::query(&create_statement).execute(&pool).await?;

    let copy_options = schema.copy_options(DB_SCHEMA);
    let records_loaded = loader.load_data(copy_options, &pool).await?;

    assert_eq!(2000_u64, records_loaded);

    Ok(())
}

#[tokio::test]
async fn shapefile_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    let expected_table_name = "shape_data_test";
    let expected_column_names = [
        ("item_id", ColumnType::Text),
        ("ai_id", ColumnType::Number),
        ("int_doc_id", ColumnType::Number),
        ("si_type", ColumnType::Text),
        ("si_cat", ColumnType::Text),
        ("si_id", ColumnType::Number),
        ("si_cat_des", ColumnType::Text),
        ("si_type_de", ColumnType::Text),
        ("ai_name", ColumnType::Text),
        ("ai_program", ColumnType::Text),
        ("ai_prg_cod", ColumnType::Text),
        ("ic_site_ty", ColumnType::Text),
        ("ic_id", ColumnType::Text),
        ("ic_name", ColumnType::Text),
        ("control_ty", ColumnType::Text),
        ("acreage", ColumnType::Real),
        ("parcel_lis", ColumnType::Text),
        ("bond_apprp", ColumnType::Text),
        ("ic_recordi", ColumnType::Text),
        ("inspection", ColumnType::Text),
        ("ic_signed", ColumnType::Date),
        ("ic_recorde", ColumnType::Date),
        ("ic_termina", ColumnType::Date),
        ("count_insp", ColumnType::Real),
        ("last_inspe", ColumnType::Date),
        ("comments", ColumnType::Text),
        ("address1", ColumnType::Text),
        ("address2", ColumnType::Text),
        ("city_name", ColumnType::Text),
        ("state_code", ColumnType::Text),
        ("zip_code", ColumnType::Text),
        ("county_nam", ColumnType::Text),
        ("county_cod", ColumnType::Text),
        ("ctu_code", ColumnType::Text),
        ("ctu_name", ColumnType::Text),
        ("cong_dist", ColumnType::Text),
        ("house_dist", ColumnType::Text),
        ("senate_dis", ColumnType::Text),
        ("huc8", ColumnType::Text),
        ("huc8_name", ColumnType::Text),
        ("huc10", ColumnType::Text),
        ("huc12", ColumnType::Text),
        ("huc12_name", ColumnType::Text),
        ("dwsma_code", ColumnType::Text),
        ("dwsma_name", ColumnType::Text),
        ("loc_desc", ColumnType::Text),
        ("latitude", ColumnType::Real),
        ("longitude", ColumnType::Real),
        ("method_cod", ColumnType::Text),
        ("method_des", ColumnType::Text),
        ("ref_code", ColumnType::Text),
        ("ref_desc", ColumnType::Text),
        ("collection", ColumnType::Date),
        ("tmsp_creat", ColumnType::Date),
        ("tmsp_updt", ColumnType::Date),
        ("geometry", ColumnType::Geometry),
    ];

    let loader = DataLoader::new(&json!({
        "file_path": "tests/shape-data-test/shape_data_test.shp",
    }))?;
    let schema = loader.schema().await?;

    assert_eq!(expected_table_name, schema.table_name());

    let fields = schema.columns();
    assert_eq!(expected_column_names.len(), fields.len());
    for (ex_field, field) in expected_column_names.iter().zip(fields) {
        assert_eq!(ex_field.0, field.name());
        assert_eq!(&ex_field.1, field.column_type());
    }

    let pool = create_db_pool().await?;
    sqlx::query(&format!(
        "drop table if exists {}.{}",
        DB_SCHEMA,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(DB_SCHEMA);
    sqlx::query(&create_statement).execute(&pool).await?;

    let copy_options = schema.copy_options(DB_SCHEMA);
    let records_loaded = loader.load_data(copy_options, &pool).await?;

    assert_eq!(1244_u64, records_loaded);

    Ok(())
}

#[tokio::test]
async fn geojson_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    //https://arcgis.metc.state.mn.us/server/rest/services/ESWastewater/RainGaugeSites/FeatureServer
    let expected_table_name = "geojson_data_test";
    let expected_column_names = [
        ("objectid", ColumnType::Number),
        ("uniqueid", ColumnType::Number),
        ("featurelabel", ColumnType::Text),
        ("sitename", ColumnType::Text),
        ("featurestatus", ColumnType::Text),
        ("featureowner", ColumnType::Text),
        ("wwtp", ColumnType::Text),
        ("locationdescription", ColumnType::Text),
        ("address", ColumnType::Text),
        ("ctu_name", ColumnType::Text),
        ("co_name", ColumnType::Text),
        ("zip", ColumnType::Number),
        ("notes", ColumnType::Text),
        ("gpsdate", ColumnType::Text),
        ("gpsaccuracy", ColumnType::Text),
        ("gpsdatasource", ColumnType::Text),
        ("gpsnorthing", ColumnType::Number),
        ("gpseasting", ColumnType::Number),
        ("yearopened", ColumnType::Number),
        ("yearclosed", ColumnType::Text),
        ("datasource", ColumnType::Text),
        ("datasourcedate", ColumnType::Number),
        ("assetid", ColumnType::Text),
        ("parentid", ColumnType::Text),
        ("eamasseturl", ColumnType::Text),
        ("majorwatershed", ColumnType::Text),
        ("secondarywatershed", ColumnType::Text),
        ("created_user", ColumnType::Text),
        ("created_date", ColumnType::Number),
        ("last_edited_user", ColumnType::Text),
        ("last_edited_date", ColumnType::Number),
        ("globalid", ColumnType::Text),
        ("featurestatus_desc", ColumnType::Text),
        ("featureowner_desc", ColumnType::Text),
        ("wwtp_desc", ColumnType::Text),
        ("ctu_name_desc", ColumnType::Text),
        ("co_name_desc", ColumnType::Text),
        ("datasource_desc", ColumnType::Text),
        ("geometry", ColumnType::Geometry),
    ];

    let loader = DataLoader::new(&json!({
        "file_path": "tests/geojson data test.geojson",
    }))?;
    let schema = loader.schema().await?;

    assert_eq!(expected_table_name, schema.table_name());

    let fields = schema.columns();
    assert_eq!(expected_column_names.len(), fields.len());
    for (ex_field, field) in expected_column_names.iter().zip(fields) {
        assert_eq!(ex_field.0, field.name());
        assert_eq!(&ex_field.1, field.column_type(), "field = {}", ex_field.0);
    }

    let pool = create_db_pool().await?;
    sqlx::query(&format!(
        "drop table if exists {}.{}",
        DB_SCHEMA,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(DB_SCHEMA);
    sqlx::query(&create_statement).execute(&pool).await?;

    let copy_options = schema.copy_options(DB_SCHEMA);
    let records_loaded = loader.load_data(copy_options, &pool).await?;

    assert_eq!(26_u64, records_loaded);

    Ok(())
}

#[tokio::test]
async fn parquet_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    //https://arcgis.metc.state.mn.us/server/rest/services/ESWastewater/RainGaugeSites/FeatureServer
    let expected_table_name = "parquet_data_test";
    let expected_column_names = [
        ("objectid", ColumnType::BigInt),
        ("datasummary", ColumnType::Text),
        ("api_number", ColumnType::BigInt),
        ("comp_date", ColumnType::DoublePrecision),
        ("permit_number", ColumnType::Text),
        ("permit_date", ColumnType::DoublePrecision),
        ("status", ColumnType::Text),
        ("status_text", ColumnType::Text),
        ("company_name", ColumnType::Text),
        ("farm_name", ColumnType::Text),
        ("farm_num", ColumnType::Text),
        ("total_depth", ColumnType::DoublePrecision),
        ("tdformation", ColumnType::Text),
        ("tdfmtext", ColumnType::Text),
        ("ilstrat", ColumnType::Text),
        ("elevation", ColumnType::DoublePrecision),
        ("eref_def", ColumnType::Text),
        ("longitude", ColumnType::DoublePrecision),
        ("latitude", ColumnType::DoublePrecision),
        ("location", ColumnType::Text),
        ("logs", ColumnType::Text),
        ("flag_las", ColumnType::Text),
        ("flag_core_analysis", ColumnType::Text),
        ("flag_core", ColumnType::Text),
        ("flag_samples", ColumnType::Text),
        ("flag_log", ColumnType::Text),
        ("comp_date_dt", ColumnType::Timestamp),
        ("permit_date_dt", ColumnType::Timestamp),
        ("geometry", ColumnType::Geometry),
    ];

    let loader = DataLoader::new(&json!({
        "file_path": "tests/parquet data test.parquet",
    }))?;
    let schema = loader.schema().await?;

    assert_eq!(expected_table_name, schema.table_name());

    let fields = schema.columns();
    assert_eq!(expected_column_names.len(), fields.len());
    for (ex_field, field) in expected_column_names.iter().zip(fields) {
        assert_eq!(ex_field.0, field.name());
        assert_eq!(&ex_field.1, field.column_type(), "field = {}", ex_field.0);
    }

    let pool = create_db_pool().await?;
    sqlx::query(&format!(
        "drop table if exists {}.{}",
        DB_SCHEMA,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(DB_SCHEMA);
    sqlx::query(&create_statement).execute(&pool).await?;

    let copy_options = schema.copy_options(DB_SCHEMA);
    let records_loaded = loader.load_data(copy_options, &pool).await?;

    assert_eq!(10000_u64, records_loaded);

    Ok(())
}

#[tokio::test]
async fn ipc_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    let expected_table_name = "ipc_data_test";
    let expected_column_names = [
        ("facility_id", ColumnType::Text),
        ("facility_name", ColumnType::Text),
        ("address1", ColumnType::Text),
        ("address2", ColumnType::Text),
        ("city", ColumnType::Text),
        ("state", ColumnType::Text),
        ("zip", ColumnType::Text),
        ("contact", ColumnType::Text),
        ("contact_address1", ColumnType::Text),
        ("contact_address2", ColumnType::Text),
        ("contact_city", ColumnType::Text),
        ("contact_state", ColumnType::Text),
        ("contact_zip", ColumnType::Text),
        ("tank_id", ColumnType::Text),
        ("compartment_tank", ColumnType::BigInt),
        ("manifold_tank", ColumnType::BigInt),
        ("main_tank", ColumnType::BigInt),
        ("root_tank_id", ColumnType::BigInt),
        ("installation_date", ColumnType::TimestampWithZone),
        ("perm_close_date", ColumnType::TimestampWithZone),
        ("capacity", ColumnType::BigInt),
        ("commercial", ColumnType::Boolean),
        ("regulated", ColumnType::Boolean),
        ("product_name", ColumnType::Text),
        ("overfill_protection_name", ColumnType::Text),
        ("spill_protection_name", ColumnType::Text),
        ("leak_detection_name", ColumnType::Text),
        ("tank_constr_name", ColumnType::Text),
        ("piping_constr_name", ColumnType::Text),
        ("piping_system_name", ColumnType::Text),
        ("other_cp_tank", ColumnType::SmallInt),
        ("other_cp_name", ColumnType::SmallInt),
        ("tank_status_name", ColumnType::Text),
        ("fips_county_desc", ColumnType::Text),
        ("latitude", ColumnType::DoublePrecision),
        ("longitude", ColumnType::DoublePrecision),
        ("tankcertno", ColumnType::BigInt),
        ("fr_bus_name", ColumnType::Text),
        ("fr_amt", ColumnType::BigInt),
        ("fr_desc", ColumnType::Text),
        ("last_update_date", ColumnType::TimestampWithZone),
        ("certno", ColumnType::Text),
    ];

    let loader = DataLoader::new(&json!({
        "file_path": "tests/ipc data test.ipc",
    }))?;
    let schema = loader.schema().await?;

    assert_eq!(expected_table_name, schema.table_name());

    let fields = schema.columns();
    assert_eq!(expected_column_names.len(), fields.len());
    for (ex_field, field) in expected_column_names.iter().zip(fields) {
        assert_eq!(ex_field.0, field.name());
        assert_eq!(&ex_field.1, field.column_type());
    }

    let pool = create_db_pool().await?;
    sqlx::query(&format!(
        "drop table if exists {}.{}",
        DB_SCHEMA,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(DB_SCHEMA);
    sqlx::query(&create_statement).execute(&pool).await?;

    let copy_options = schema.copy_options(DB_SCHEMA);
    let records_loaded = loader.load_data(copy_options, &pool).await?;

    assert_eq!(299_u64, records_loaded);

    Ok(())
}

#[tokio::test]
async fn arcgis_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    //https://arcgis.metc.state.mn.us/server/rest/services/ESWastewater/RainGaugeSites/FeatureServer
    let expected_table_name = "raingaugesites";
    let expected_column_names = [
        ("objectid", ColumnType::Integer),
        ("uniqueid", ColumnType::Integer),
        ("featurelabel", ColumnType::Text),
        ("sitename", ColumnType::Text),
        ("featurestatus", ColumnType::Text),
        ("featureowner", ColumnType::Text),
        ("wwtp", ColumnType::Text),
        ("locationdescription", ColumnType::Text),
        ("address", ColumnType::Text),
        ("ctu_name", ColumnType::Text),
        ("co_name", ColumnType::Text),
        ("zip", ColumnType::Integer),
        ("notes", ColumnType::Text),
        ("gpsdate", ColumnType::Timestamp),
        ("gpsaccuracy", ColumnType::DoublePrecision),
        ("gpsdatasource", ColumnType::Text),
        ("gpsnorthing", ColumnType::DoublePrecision),
        ("gpseasting", ColumnType::DoublePrecision),
        ("yearopened", ColumnType::Integer),
        ("yearclosed", ColumnType::Integer),
        ("datasource", ColumnType::Text),
        ("datasourcedate", ColumnType::Timestamp),
        ("assetid", ColumnType::Text),
        ("parentid", ColumnType::Text),
        ("eamasseturl", ColumnType::Text),
        ("majorwatershed", ColumnType::Text),
        ("secondarywatershed", ColumnType::Text),
        ("created_user", ColumnType::Text),
        ("created_date", ColumnType::Timestamp),
        ("last_edited_user", ColumnType::Text),
        ("last_edited_date", ColumnType::Timestamp),
        ("globalid", ColumnType::UUID),
        ("geometry", ColumnType::Geometry),
    ];

    let loader = DataLoader::new(&json!({
        "url": "https://arcgis.metc.state.mn.us/server/rest/services/ESWastewater/RainGaugeSites/FeatureServer/0",
    }))?;
    let schema = loader.schema().await?;

    assert_eq!(expected_table_name, schema.table_name());

    let fields = schema.columns();
    assert_eq!(expected_column_names.len(), fields.len());
    for (ex_field, field) in expected_column_names.iter().zip(fields) {
        assert_eq!(ex_field.0, field.name());
        assert_eq!(&ex_field.1, field.column_type(), "field = {}", ex_field.0);
    }

    let pool = create_db_pool().await?;
    sqlx::query(&format!(
        "drop table if exists {}.{}",
        DB_SCHEMA,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(DB_SCHEMA);
    sqlx::query(&create_statement).execute(&pool).await?;

    let copy_options = schema.copy_options(DB_SCHEMA);
    let records_loaded = loader.load_data(copy_options, &pool).await?;

    assert_eq!(26_u64, records_loaded);

    Ok(())
}

#[tokio::test]
async fn avro_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    let expected_table_name = "avro_data_test";
    let expected_column_names = [
        ("int", ColumnType::Integer),
        ("long", ColumnType::BigInt),
        ("string", ColumnType::Text),
        ("boolean", ColumnType::Boolean),
        ("float", ColumnType::Real),
        ("double", ColumnType::DoublePrecision),
        ("bytes", ColumnType::SmallIntArray),
        ("intarray", ColumnType::Json),
        ("map", ColumnType::Json),
        ("union", ColumnType::Text),
        ("enum", ColumnType::Text),
        ("record", ColumnType::Json),
        ("fixed", ColumnType::SmallIntArray),
        ("decimal", ColumnType::SmallIntArray),
        ("uuid", ColumnType::UUID),
        ("date", ColumnType::Date),
        ("time_millis", ColumnType::Time),
        ("time_micros", ColumnType::Time),
        ("timestamp_millis", ColumnType::Timestamp),
        ("timestamp_micros", ColumnType::Timestamp),
    ];

    let loader = DataLoader::new(&json!({
        "file_path": "tests/avro data test.avro",
    }))?;
    let schema = loader.schema().await?;

    assert_eq!(expected_table_name, schema.table_name());

    let fields = schema.columns();
    assert_eq!(expected_column_names.len(), fields.len());
    for (ex_field, field) in expected_column_names.iter().zip(fields) {
        assert_eq!(ex_field.0, field.name());
        assert_eq!(&ex_field.1, field.column_type(), "field = {}", ex_field.0);
    }

    let pool = create_db_pool().await?;
    sqlx::query(&format!(
        "drop table if exists {}.{}",
        DB_SCHEMA,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(DB_SCHEMA);
    if let Err(error) = sqlx::query(&create_statement).execute(&pool).await {
        return Err(format!(
            "Error attempting to execute create statement \"{}\".\n{}",
            create_statement, error
        )
        .into());
    }

    let copy_options = schema.copy_options(DB_SCHEMA);
    let records_loaded = loader.load_data(copy_options, &pool).await?;

    assert_eq!(20_u64, records_loaded);

    Ok(())
}
