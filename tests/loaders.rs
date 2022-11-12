use geoflow_rs::{
    bulk_loading::{
        analyze::{ColumnType, SchemaAnalyzer},
        delimited::DelimitedDataOptions,
        load::DataLoader,
    },
    database::create_db_pool,
};

#[tokio::test]
async fn delimited_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    let check_table_name = "expected_delimited_data_test";
    let db_schema = "geoflow";
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

    let mut path = std::env::current_dir()?;
    path.push("tests/delimited data test.csv");
    let options = DelimitedDataOptions::new(path, ',', true);
    let analyzer = SchemaAnalyzer::from_delimited(options.clone());
    let schema = analyzer.schema()?;

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
        db_schema,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(db_schema);
    sqlx::query(&create_statement).execute(&pool).await?;

    let loader = DataLoader::from_delimited(options);
    let copy_options = schema.copy_options(db_schema);
    let records_loaded = loader.load_data(copy_options, pool.clone()).await?;

    assert_eq!(299_u64, records_loaded);

    let except_check: i64 = sqlx::query_scalar(&format!(
        "select count(0) from (select * from {}.{} except select * from {}.{}) t1",
        db_schema,
        check_table_name,
        db_schema,
        schema.table_name()
    ))
    .fetch_one(&pool)
    .await?;

    assert_eq!(0_i64, except_check);

    Ok(())
}
