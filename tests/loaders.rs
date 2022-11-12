use geoflow_rs::{
    bulk_loading::{
        analyze::{ColumnType, SchemaAnalyzer},
        delimited::DelimitedDataOptions,
        load::DataLoader, excel::ExcelOptions,
    },
    database::create_db_pool,
};

#[tokio::test]
async fn delimited_data_loading() -> Result<(), Box<dyn std::error::Error>> {
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

    Ok(())
}

#[tokio::test]
async fn excel_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    let db_schema = "geoflow";
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

    let mut path = std::env::current_dir()?;
    path.push("tests/excel data test.xlsx");
    let options = ExcelOptions::new(path, String::from("tblUST_DB"));
    let analyzer = SchemaAnalyzer::from_excel(options.clone());
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

    let loader = DataLoader::from_excel(options);
    let copy_options = schema.copy_options(db_schema);
    let records_loaded = loader.load_data(copy_options, pool.clone()).await?;

    assert_eq!(2000_u64, records_loaded);

    Ok(())
}
