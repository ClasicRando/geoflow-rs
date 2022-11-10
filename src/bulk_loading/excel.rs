use calamine::{open_workbook_auto, DataType, Range, Reader};
use std::path::PathBuf;
use tokio::sync::mpsc::{error::SendError, Sender};

use super::{
    analyze::{ColumnMetadata, ColumnType, Schema, SchemaParser},
    error::{BulkDataError, BulkDataResult},
    load::{csv_values_to_string, DataParser},
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

    fn sheet(&self) -> BulkDataResult<Range<DataType>> {
        let mut workbook = open_workbook_auto(&self.file_path)?;
        let sheet = match workbook.worksheet_range(&self.sheet_name) {
            Some(Ok(sheet)) => sheet,
            _ => {
                return Err(format!(
                    "Could not find sheet \"{}\" in {:?}",
                    &self.sheet_name, &self.file_path
                )
                .into())
            }
        };
        Ok(sheet)
    }
}

impl DataFileOptions for ExcelOptions {}

pub struct ExcelSchemaParser(ExcelOptions);

impl SchemaParser for ExcelSchemaParser {
    type Options = ExcelOptions;

    fn new(options: Self::Options) -> Self
    where
        Self: Sized,
    {
        Self(options)
    }

    fn schema(&self) -> BulkDataResult<Schema> {
        let Some(table_name) = self.0.file_path.file_name().and_then(|f| f.to_str()) else {
            return Err(format!("Could not get filename for \"{:?}\"", &self.0.file_path).into())
        };
        let sheet = self.0.sheet()?;
        let Some(header_row) = sheet.rows().next() else {
            return Err(format!(
                "Could not find header row in \"{}\" of {:?}",
                &self.0.sheet_name, &self.0.file_path
            ).into())
        };
        let columns = header_row
            .iter()
            .enumerate()
            .map(|(i, field)| -> BulkDataResult<ColumnMetadata> {
                let field_value = map_excel_value(field)?;
                ColumnMetadata::new(field_value, i, ColumnType::Text)
            })
            .collect::<Result<Vec<ColumnMetadata>, _>>()?;
        Schema::new(table_name, columns)
    }
}

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

pub struct ExcelDataParser(ExcelOptions);

impl ExcelDataParser {
    pub fn new(options: ExcelOptions) -> Self {
        Self(options)
    }
}

#[async_trait::async_trait]
impl DataParser for ExcelDataParser {
    type Options = ExcelOptions;

    fn options(&self) -> &Self::Options {
        &self.0
    }

    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        let sheet = match self.0.sheet() {
            Ok(sheet) => sheet,
            Err(error) => return record_channel.send(Err(error)).await.err(),
        };
        let mut rows = sheet.rows();
        let header = match rows.next() {
            Some(row) => row,
            None => {
                return record_channel
                    .send(Err(format!(
                        "Could not find a header row for excel file {:?}",
                        self.0.file_path,
                    )
                    .into()))
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
                    .send(Err(format!(
                        "Excel row {} has cells that contain errors",
                        row_num + 1
                    ).into()))
                    .await
                    .err();
            };
            if row_values.len() != header_size {
                return record_channel
                    .send(Err(format!(
                        "Excel row {} has {} values but expected {}",
                        row_num + 1,
                        row_values.len(),
                        header_size
                    )
                    .into()))
                    .await
                    .err();
            }
            let result = record_channel
                .send(Ok(csv_values_to_string(row_values)))
                .await;
            if let Err(error) = result {
                return Some(error);
            }
        }
        None
    }
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
