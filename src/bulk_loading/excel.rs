use calamine::{open_workbook_auto, DataType, Reader};
use std::path::{Path, PathBuf};

use super::{
    error::{BulkDataError, BulkDataResult},
    loader::{copy_csv_values, CopyPipe, CopyResult},
    options::DataFileOptions,
};

pub struct ExcelOptions {
    file_path: PathBuf,
    sheet_name: String,
}

impl ExcelOptions {
    pub fn new(file_path: PathBuf, sheet_name: String) -> Self {
        Self {
            file_path,
            sheet_name,
        }
    }
}

impl DataFileOptions for ExcelOptions {}

pub fn map_excel_value(value: &DataType) -> BulkDataResult<String> {
    Ok(match value {
        DataType::String(s) => s.replace("_x000d_", "\n").replace("_x000a_", "\r"),
        DataType::DateTime(_) => {
            let formatted_datetime = value
                .as_datetime()
                .ok_or(format!(
                    "Cell error. Should be datetime but found something else. {}",
                    value
                ))?
                .format("%Y-%m-%d %H:%M:%S");
            format!("{}", formatted_datetime)
        }
        DataType::Error(e) => return Err(BulkDataError::Generic(format!("Cell error, {}", e))),
        DataType::Empty => String::new(),
        _ => format!("{}", value),
    })
}

pub async fn load_excel_data(copy: &mut CopyPipe, options: &ExcelOptions) -> CopyResult {
    let (file_path, sheet_name) = (&options.file_path, &options.sheet_name);
    let mut workbook = open_workbook_auto(file_path)?;
    let sheet = match workbook.worksheet_range(sheet_name) {
        Some(Ok(sheet)) => sheet,
        _ => {
            return Err(BulkDataError::Generic(format!(
                "Could not find sheet \"{}\" in {:?}",
                sheet_name, file_path
            )))
        }
    };
    let mut rows = sheet.rows();
    let header = match rows.next() {
        Some(row) => row,
        None => {
            return Err(BulkDataError::Generic(format!(
                "Could not find a header row for excel file {:?}",
                file_path
            )))
        }
    };
    let header_size = header.len();
    for (row_num, row) in rows.enumerate() {
        let row_data = row
            .iter()
            .map(|value| map_excel_value(value))
            .collect::<Result<Vec<String>, _>>()?;
        if row_data.len() != header_size {
            return Err(BulkDataError::Generic(format!(
                "Excel row {} has {} values but expected {}",
                row_num + 1,
                row_data.len(),
                header_size
            )));
        }
        copy_csv_values(copy, row_data).await?;
    }
    Ok(())
}
