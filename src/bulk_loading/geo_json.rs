use geo_types::Geometry;
use geojson::FeatureReader;
use std::{fs::File, io::BufReader};
use tokio::sync::mpsc::{error::SendError, Sender};
use wkt::ToWkt;

use super::{
    error::BulkDataResult,
    loader::{csv_values_to_string, DataParser},
    options::DefaultFileOptions,
};

pub struct GeoJsonParser {
    options: DefaultFileOptions,
    reader: FeatureReader<BufReader<File>>,
}

#[async_trait::async_trait]
impl DataParser for GeoJsonParser {
    type Options = DefaultFileOptions;

    fn new(options: Self::Options) -> BulkDataResult<Self> {
        let file = File::open(&options.file_path)?;
        let buff_reader = BufReader::new(file);
        let feature_reader = FeatureReader::from_reader(buff_reader);
        Ok(Self {
            options,
            reader: feature_reader,
        })
    }

    fn options(&self) -> &Self::Options {
        &self.options
    }

    async fn spool_records(
        mut self,
        record_channel: &mut Sender<BulkDataResult<String>>,
    ) -> Option<SendError<BulkDataResult<String>>> {
        for (i, feature) in self.reader.features().enumerate() {
            let Ok(feature) = feature else {
                return record_channel
                    .send(Err(format!("Could not obtain feature {}", &i).into()))
                    .await
                    .err();
            };
            let geom = feature
                .geometry
                .as_ref()
                .and_then(|g| Geometry::<f64>::try_from(g).ok())
                .map(|g| g.wkt_string())
                .unwrap_or_default();
            let mut csv_row: Vec<String> = feature
                .properties_iter()
                .map(|(_, value)| format!("{}", value))
                .collect();
            csv_row.push(geom);
            let csv_data = csv_values_to_string(csv_row);
            let result = record_channel.send(Ok(csv_data)).await;
            if let Err(error) = result {
                return Some(error);
            }
        }
        None
    }
}
