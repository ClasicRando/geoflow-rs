use std::{fs::File, io::BufReader};

use geo_types::Geometry;
use geojson::FeatureReader;
use wkt::ToWkt;

use super::{
    loader::{copy_csv_iter, CopyPipe, CopyResult},
    options::DefaultFileOptions,
};

pub async fn load_geo_json_data(copy: &mut CopyPipe, options: &DefaultFileOptions) -> CopyResult {
    let file = File::open(&options.file_path)?;
    let buff_reader = BufReader::new(file);
    let feature_reader = FeatureReader::from_reader(buff_reader);
    for feature in feature_reader.features() {
        let feature = feature?;
        let geom = feature
            .geometry
            .as_ref()
            .and_then(|g| Geometry::<f64>::try_from(g).ok())
            .map(|g| g.wkt_string())
            .unwrap_or_default();
        let csv_row = feature
            .properties_iter()
            .map(|(_, value)| format!("{}", value))
            .chain(std::iter::once(geom));
        copy_csv_iter(copy, csv_row).await?;
    }
    Ok(())
}
