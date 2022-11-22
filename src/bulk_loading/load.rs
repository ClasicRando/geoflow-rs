use itertools::Itertools;
use sqlx::PgPool;
use tokio::sync::mpsc::{channel as mpsc_channel, error::SendError, Sender};

use super::{
    error::{BulkDataError, BulkDataResult},
    options::DataFileOptions,
    utilities::escape_csv_string,
};

pub type BulkLoadResult = Result<u64, BulkDataError>;

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

    fn copy_statement<O: DataFileOptions>(&self, options: &O) -> String {
        format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER '{}', HEADER {}, NULL ''{})",
            self.table_name.to_lowercase(),
            self.columns.join(","),
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
    csv_iter: I,
) -> BulkDataResult<String> {
    let mut csv_data = String::new();
    for s in csv_iter {
        let csv_value = s?;
        csv_data.push_str(&escape_csv_string(csv_value));
        csv_data.push(',');
    }
    csv_data.pop();
    csv_data.push('\n');
    Ok(csv_data)
}

pub fn csv_iter_to_string<I: Iterator<Item = String>>(csv_iter: I) -> String {
    let mut csv_data = csv_iter.map(escape_csv_string).join(",");
    csv_data.push('\n');
    csv_data
}

pub fn csv_values_to_string<I: IntoIterator<Item = String>>(csv_values: I) -> String {
    let mut csv_data = csv_values.into_iter().map(escape_csv_string).join(",");
    csv_data.push('\n');
    csv_data
}

#[async_trait::async_trait]
pub trait DataParser {
    type Options: DataFileOptions;

    fn options(&self) -> &Self::Options;
    async fn spool_records(
        self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>>;
}

pub struct DataLoader<P: DataParser + Send + Sync + 'static>(P);

impl<P: DataParser + Send + Sync + 'static> DataLoader<P> {
    pub fn new(parser: P) -> Self {
        Self(parser)
    }

    pub async fn load_data(self, copy_options: CopyOptions, pool: PgPool) -> BulkLoadResult {
        let copy_statement = copy_options.copy_statement(self.0.options());
        let mut copy = pool.copy_in_raw(&copy_statement).await?;
        let (mut tx, mut rx) = mpsc_channel(1000);
        let spool_handle = tokio::spawn(async move {
            let error = self.0.spool_records(&mut tx).await;
            drop(tx);
            error
        });
        let result = loop {
            match rx.recv().await {
                Some(msg) => match msg {
                    Ok(record) => {
                        if let Err(error) = copy.send(record.as_bytes()).await {
                            break Err(error.into());
                        }
                    }
                    Err(error) => break Err(error),
                },
                None => break Ok(()),
            }
        };
        rx.close();
        match spool_handle.await {
            Ok(Some(value)) => println!("SendError\n{:?}", value.0),
            Ok(None) => println!("Finished spool handle successfully"),
            Err(error) => println!("Error trying to finish the spool handle\n{}", error),
        }
        match result {
            Ok(_) => Ok(copy.finish().await?),
            Err(error) => {
                copy.abort(format!("{}", error)).await?;
                Err(error)
            }
        }
    }
}
