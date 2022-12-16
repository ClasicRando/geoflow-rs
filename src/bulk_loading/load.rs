use itertools::Itertools;
use tokio::sync::mpsc::{error::SendError, Sender};

use super::{
    error::{BulkDataError, BulkDataResult},
    options::DataOptions,
    utilities::escape_csv_string,
};

pub type BulkLoadResult = Result<u64, BulkDataError>;
pub type RecordSpoolResult = Option<SendError<BulkDataResult<String>>>;
pub type RecordSpoolChannel = Sender<BulkDataResult<String>>;

pub struct CopyOptions {
    table_name: String,
    columns: Vec<String>,
}

impl CopyOptions {
    pub fn new(table_name: &str, columns: &[&str]) -> Self {
        Self {
            table_name: table_name.to_owned(),
            columns: columns.iter().map(|s| s.to_string()).collect_vec(),
        }
    }

    pub fn from_vec(table_name: String, columns: Vec<String>) -> Self {
        Self {
            table_name,
            columns,
        }
    }

    pub fn copy_statement<O: DataOptions>(&self, options: &O) -> String {
        format!(
            "COPY {} (\"{}\") FROM STDIN WITH (FORMAT csv, DELIMITER '{}', HEADER {}, NULL ''{})",
            self.table_name.to_lowercase(),
            self.columns.join("\",\""),
            options.delimiter(),
            if *options.header() { "true" } else { "false" },
            if *options.qualified() {
                ", QUOTE '\"', ESCAPE '\"'"
            } else {
                ""
            }
        )
    }
}

pub fn csv_result_iter_to_string<I: Iterator<Item = BulkDataResult<String>>>(
    mut csv_iter: I,
) -> BulkDataResult<String> {
    let Some(first_value) = csv_iter.next() else {
        return Ok(String::new())
    };
    let mut csv_data = first_value?;
    for s in csv_iter {
        csv_data.push(',');
        let csv_value = s?;
        csv_data.push_str(&escape_csv_string(csv_value));
    }
    csv_data.push('\n');
    Ok(csv_data)
}

pub fn csv_iter_to_string<I: Iterator<Item = String>>(csv_iter: I) -> String {
    let mut csv_data = csv_iter.map(escape_csv_string).join(",");
    csv_data.push('\n');
    csv_data
}
