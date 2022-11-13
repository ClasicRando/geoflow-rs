use geoflow_rs::{
    bulk_loading::{
        analyze::{ColumnType, SchemaAnalyzer},
        delimited::DelimitedDataOptions,
        excel::ExcelOptions,
        load::DataLoader,
        shape::ShapeDataOptions,
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

#[tokio::test]
async fn shapefile_data_loading() -> Result<(), Box<dyn std::error::Error>> {
    let db_schema = "geoflow";
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

    let mut path = std::env::current_dir()?;
    path.push("tests/shape-data-test/shape_data_test.shp");
    let options = ShapeDataOptions::new(path);
    let analyzer = SchemaAnalyzer::from_shapefile(options.clone());
    let schema = analyzer.schema()?;

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
        db_schema,
        schema.table_name()
    ))
    .execute(&pool)
    .await?;
    let create_statement = schema.create_statement(db_schema);
    sqlx::query(&create_statement).execute(&pool).await?;

    let loader = DataLoader::from_shapefile(options);
    let copy_options = schema.copy_options(db_schema);
    let records_loaded = loader.load_data(copy_options, pool.clone()).await?;

    assert_eq!(1244_u64, records_loaded);

    Ok(())
}
