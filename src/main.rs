use itertools::Itertools;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use std::env;

mod bulk_loading;
mod database;
mod server;

use bulk_loading::loader::{BulkDataLoader, BulkLoadResult, CopyOptions};

pub fn db_options() -> Result<PgConnectOptions, Box<dyn std::error::Error>> {
    let we_host_address =
        env::var("WE_HOST").map_err(|_| "Missing WE_HOST environment variable".to_string())?;
    let we_db_name =
        env::var("WE_DB").map_err(|_| "Missing WE_DB environment variable".to_string())?;
    let we_db_user =
        env::var("WE_USER").map_err(|_| "Missing WE_USER environment variable".to_string())?;
    let we_db_password = env::var("WE_PASSWORD")
        .map_err(|_| "Missing WE_PASSWORD environment variable".to_string())?;
    let options = PgConnectOptions::new()
        .host(&we_host_address)
        .database(&we_db_name)
        .username(&we_db_user)
        .password(&we_db_password);
    Ok(options)
}

pub async fn create_db_pool() -> Result<PgPool, Box<dyn std::error::Error>> {
    let options = db_options()?;
    let pool = PgPoolOptions::new()
        .min_connections(10)
        .max_connections(20)
        .connect_with(options)
        .await?;
    Ok(pool)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    let pool = create_db_pool().await?;
    let loader = BulkDataLoader::DelimitedData {
        file_path:
            "/home/steven/IdeaProjects/exekutor/loader/src/test/resources/flat_file_test.csv".into(),
        delimiter: ',',
        qualified: true,
    };
    loader
        .load_data(
            CopyOptions::new(
                "test_table",
                &vec![
                    "Facility_Id",
                    "Facility_Name",
                    "Address1",
                    "Address2",
                    "City",
                    "State",
                    "Zip",
                    "Contact",
                    "Contact_Address1",
                    "Contact_Address2",
                    "Contact_City",
                    "Contact_State",
                    "Contact_Zip",
                    "Tank_Id",
                    "Compartment_Tank",
                    "Manifold_Tank",
                    "Main_Tank",
                    "Root_Tank_Id",
                    "Installation_Date",
                    "Perm_Close_Date",
                    "Capacity",
                    "Commercial",
                    "Regulated",
                    "Product_Name",
                    "OverFill_Protection_name",
                    "Spill_Protection_name",
                    "Leak_Detection_name",
                    "Tank_Constr_Name",
                    "Piping_Constr_Name",
                    "Piping_System_Name",
                    "Other_Cp_Tank",
                    "Other_Cp_Name",
                    "Tank_Status_Name",
                    "Fips_County_Desc",
                    "Latitude",
                    "Longitude",
                    "TankCertNo",
                    "FR_Bus_Name",
                    "FR_Amt",
                    "FR_Desc",
                    "Last_Update_Date",
                    "CertNo",
                ],
            ),
            pool,
        )
        .await?;
    Ok(())
}
