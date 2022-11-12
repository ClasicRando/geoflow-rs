#![allow(dead_code)]

use geoflow_rs::bulk_loading::delimited::DelimitedDataOptions;
use geoflow_rs::bulk_loading::load::{CopyOptions, DataLoader};
use geoflow_rs::database::create_db_pool;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = create_db_pool().await?;
    let mut path = PathBuf::new();
    path.push("/home/steventhomson/Downloads/NC_Tanks_Text/tblAllTanks.txt");
    let options = DelimitedDataOptions::new(path, ',', true);
    let loader = DataLoader::from_delimited(options);
    let copy_options = CopyOptions::new(
        "public.tank_nc",
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
            "Tank_Status_Key",
            "Compartment_Tank",
            "Manifold_Tank",
            "Main_Tank",
            "Root_Tank_Id",
            "Installation_Date",
            "Perm_Close_Date",
            "Capacity",
            "Commercial",
            "Regulated",
            "Product_Key",
            "Tank_Constr_Key",
            "Piping_Constr_Key",
            "Piping_System_Key",
            "Other_Cp_Tank",
            "Product_Name",
            "Tank_Status_Name",
            "Fips_County_Desc",
            "Latitude",
            "Longitude",
            "OverFill_Protection_name",
            "Spill_Protection_name",
            "Leak_Detection_name",
            "OverFill_Protection_key",
            "Spill_Protection_key",
            "Leak_Detection_key",
            "FR_Bus_Name",
            "FR_Desc",
            "FR_Amt",
            "Last_Update_Date",
            "CertNo",
            "Piping_System_Name",
            "Piping_Constr_Name",
            "Other_Cp_Name",
            "TankCertNo",
            "Tank_Constr_Name",
        ],
    );
    let record_count = loader.load_data(copy_options, pool).await?;
    println!("Copied {} records!", record_count);
    Ok(())
}
