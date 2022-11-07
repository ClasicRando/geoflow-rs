use calamine::{open_workbook_auto, DataType, Range, Reader};
use std::path::PathBuf;
use tokio::sync::mpsc::{error::SendError, Sender};

use super::{
    error::{BulkDataError, BulkDataResult},
    loader::{copy_csv_values, csv_values_to_string, CopyPipe, CopyResult, DataParser},
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

pub struct ExcelDataParser {
    options: ExcelOptions,
    sheet: Range<DataType>,
}

#[async_trait::async_trait]
impl DataParser for ExcelDataParser {
    type Options = ExcelOptions;

    fn new(options: Self::Options) -> BulkDataResult<Self> {
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
        Ok(Self {
            options: options,
            sheet: sheet,
        })
    }

    fn options(&self) -> &Self::Options {
        &self.options
    }

    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let mut rows = self.sheet.rows();
        let header = match rows.next() {
            Some(row) => row,
            None => {
                return record_channel
                    .send(Err(BulkDataError::Generic(format!(
                        "Could not find a header row for excel file {:?}",
                        self.options.file_path,
                    ))))
                    .await
                    .err();
            }
        };
        let header_size = header.len();
        for (row_num, row) in rows.enumerate() {
            let row_data = row
                .iter()
                .map(|value| map_excel_value(value))
                .collect::<Result<Vec<String>, _>>();
            let Ok(row_values) = row_data else {
                return record_channel
                    .send(Err(BulkDataError::Generic(format!(
                        "Excel row {} has cells that contain errors",
                        row_num + 1
                    ))))
                    .await
                    .err();
            };
            if row_values.len() != header_size {
                return record_channel
                    .send(Err(BulkDataError::Generic(format!(
                        "Excel row {} has {} values but expected {}",
                        row_num + 1,
                        row_values.len(),
                        header_size
                    ))))
                    .await
                    .err();
            }
            let result = record_channel
                .send(Ok(csv_values_to_string(row_values)))
                .await;
            if let Err(error) = result {
                return Some(error)
            }
        }
        None
    }
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

#[cfg(test)]
mod tests {
    use calamine::DataType;

    use super::*;

    #[test]
    fn map_excel_value_should_return_integer_string_when_int() -> BulkDataResult<()> {
        let value = DataType::Int(25_i64);

        let actual = map_excel_value(&value)?;

        assert_eq!("25", actual);
        Ok(())
    }

    #[test]
    fn map_excel_value_should_return_numeric_string_when_float() -> BulkDataResult<()> {
        let value = DataType::Float(0.025698);

        let actual = map_excel_value(&value)?;

        assert_eq!("0.025698", actual);
        Ok(())
    }

    #[test]
    fn map_excel_value_should_return_same_string_when_string() -> BulkDataResult<()> {
        let expected = String::from("This is a test");
        let value = DataType::String(expected.to_owned());

        let actual = map_excel_value(&value)?;

        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn map_excel_value_should_return_string_with_newline_fixed_when_string_with_newline(
    ) -> BulkDataResult<()> {
        let expected = String::from("This is a test\nSecond line");
        let string = String::from("This is a test_x000d_Second line");
        let value = DataType::String(string);

        let actual = map_excel_value(&value)?;

        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn map_excel_value_should_return_string_with_carriage_return_fixed_when_string_with_carriage_return(
    ) -> BulkDataResult<()> {
        let expected = String::from("This is a test\rSecond line");
        let string = String::from("This is a test_x000a_Second line");
        let value = DataType::String(string);

        let actual = map_excel_value(&value)?;

        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn map_excel_value_should_return_true_string_when_bool_true() -> BulkDataResult<()> {
        let value = DataType::Bool(true);

        let actual = map_excel_value(&value)?;

        assert_eq!("true", actual);
        Ok(())
    }

    #[test]
    fn map_excel_value_should_return_false_string_when_bool_false() -> BulkDataResult<()> {
        let value = DataType::Bool(false);

        let actual = map_excel_value(&value)?;

        assert_eq!("false", actual);
        Ok(())
    }

    #[test]
    fn map_excel_value_should_return_date_string_when_date() -> BulkDataResult<()> {
        let days_from_epoch = 19287.;
        let seconds_from_epoch = 0.83985;
        let value = DataType::DateTime(25569. + days_from_epoch + seconds_from_epoch);

        let actual = map_excel_value(&value)?;

        assert_eq!("2022-10-22 20:09:23", actual);
        Ok(())
    }

    #[test]
    fn map_excel_value_should_return_generic_error_when_error() {
        let value = DataType::Error(calamine::CellErrorType::Div0);

        let actual = match map_excel_value(&value) {
            Ok(_) => panic!("Test of map_excel_value should have returned an Err variant"),
            Err(error) => match error {
                BulkDataError::Generic(s) => s,
                _ => panic!(
                    "Test of map_excel_value should have returned a Generic BulkDataError Variant"
                ),
            },
        };

        assert_eq!("Cell error, #DIV/0!", actual);
    }
}
