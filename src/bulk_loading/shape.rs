use shapefile::{dbase::FieldValue, Shape};
use wkt::ToWkt;

use super::{
    loader::{copy_csv_iter, CopyPipe, CopyResult},
    options::DefaultFileOptions,
};

fn map_field_value(value: FieldValue) -> String {
    match value {
        FieldValue::Character(str) => str.unwrap_or_default(),
        FieldValue::Numeric(n) => n.map(|f| f.to_string()).unwrap_or_default(),
        FieldValue::Logical(l) => l.map(|b| b.to_string()).unwrap_or_default(),
        FieldValue::Date(date) => date
            .map(|d| format!("{}-{}-{}", d.year(), d.month(), d.day()))
            .unwrap_or_default(),
        FieldValue::Float(f) => f.map(|f| f.to_string()).unwrap_or("".into()),
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Currency(c) => c.to_string(),
        FieldValue::DateTime(dt) => {
            let date = dt.date();
            let time = dt.time();
            format!(
                "{}-{}-{} {}:{}:{}",
                date.year(),
                date.month(),
                date.day(),
                time.hours(),
                time.minutes(),
                time.seconds()
            )
        }
        FieldValue::Double(d) => d.to_string(),
        FieldValue::Memo(m) => m,
    }
}

pub async fn load_shape_data(copy: &mut CopyPipe, options: &DefaultFileOptions) -> CopyResult {
    let mut reader = shapefile::Reader::from_path(&options.file_path)?;
    for feature in reader.iter_shapes_and_records() {
        let (shape, record) = feature?;
        let wkt = match shape {
            Shape::NullShape => String::new(),
            _ => {
                let geo = geo_types::Geometry::<f64>::try_from(shape)?;
                format!("{}", geo.wkt_string())
            }
        };
        let csv_row = record
            .into_iter()
            .map(|(_, value)| map_field_value(value))
            .chain(std::iter::once(wkt));
        copy_csv_iter(copy, csv_row).await?;
    }
    Ok(())
}
