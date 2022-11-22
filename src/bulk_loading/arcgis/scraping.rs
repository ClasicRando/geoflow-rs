use crate::bulk_loading::error::{BulkDataError, BulkDataResult};
use geojson::{feature::Id, Feature, FeatureCollection, Geometry, Value as GeomValue, GeoJson};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value};

static MAX_RETRY: i32 = 5;

#[derive(Debug, PartialEq, Eq)]
pub enum QueryFormat {
    GeoJSON,
    JSON,
    NotSupported(String),
}

impl QueryFormat {
    pub fn as_str(&self) -> &str {
        match self {
            Self::GeoJSON => "geojson",
            Self::JSON => "json",
            Self::NotSupported(name) => name.as_str(),
        }
    }

    async fn try_query(&self, client: &Client, query: &str) -> BulkDataResult<FeatureCollection> {
        let response = client.get(query).send().await?;
        if response.status() != 200 {
            return Err((query, response.status()).into());
        }
        let feature_collection = match self {
            Self::GeoJSON => match response.json::<GeoJson>().await? {
                GeoJson::FeatureCollection(collection) => collection,
                GeoJson::Geometry(_) => return Err("Expected a Feature collect but got Geometry".into()),
                GeoJson::Feature(_) => return Err("Expected a Feature collect but got Feature".into()),
            }
            Self::JSON => response.json::<JsonQueryResponse>().await?.into(),
            Self::NotSupported(name) => {
                return Err(
                    format!("Cannot read the query response for format \"{}\"", name).into(),
                )
            }
        };
        Ok(feature_collection)
    }
}

impl From<&str> for QueryFormat {
    fn from(str: &str) -> Self {
        let formats = str.to_lowercase();
        if formats.contains("geojson") {
            Self::GeoJSON
        } else if formats.contains("json") {
            Self::JSON
        } else {
            let format = match formats.split(',').next() {
                Some(f) => f.to_owned(),
                None => formats,
            };
            Self::NotSupported(format)
        }
    }
}

#[derive(Deserialize)]
enum JsonQueryGeometry {
    Point { x: f64, y: f64 },
}

impl From<JsonQueryGeometry> for Geometry {
    fn from(geom: JsonQueryGeometry) -> Self {
        match geom {
            JsonQueryGeometry::Point { x, y } => Self {
                bbox: None,
                value: GeomValue::Point(vec![x, y]),
                foreign_members: None,
            },
        }
    }
}

#[derive(Deserialize)]
struct JsonQueryFeature {
    attributes: Map<String, Value>,
    geometry: Option<JsonQueryGeometry>,
}

#[derive(Deserialize)]
struct JsonQueryResponse {
    features: Vec<JsonQueryFeature>,
}

impl From<JsonQueryResponse> for FeatureCollection {
    fn from(response: JsonQueryResponse) -> Self {
        Self {
            bbox: None,
            features: response
                .features
                .into_iter()
                .enumerate()
                .map(|(i, feature)| Feature {
                    bbox: None,
                    geometry: feature.geometry.map(|g| g.into()),
                    id: Some(Id::Number(i.into())),
                    properties: Some(feature.attributes),
                    foreign_members: None,
                })
                .collect(),
            foreign_members: None,
        }
    }
}

async fn loop_until_successful(
    client: &Client,
    query: &str,
    query_format: &QueryFormat,
) -> BulkDataResult<FeatureCollection> {
    let mut attempts = 0;
    let result = loop {
        attempts += 1;
        if attempts > MAX_RETRY {
            return Err(
                format!("Exceeded max number of retries for a query ({})", MAX_RETRY).into(),
            );
        }
        match query_format.try_query(client, query).await {
            Err(error) => match error {
                BulkDataError::ArcGis(_, _) => continue,
                _ => return Err(error),
            },
            Ok(obj) => break obj,
        }
    };
    Ok(result)
}

pub async fn fetch_query(
    client: &Client,
    query: &str,
    query_format: &QueryFormat,
) -> BulkDataResult<FeatureCollection> {
    let feature_collection = loop_until_successful(client, query, query_format).await?;
    Ok(feature_collection)
}

#[cfg(test)]
mod tests {
    use super::QueryFormat;

    #[test]
    fn query_format_as_str() {
        let query_format_geo_json = QueryFormat::GeoJSON;
        let query_format_json = QueryFormat::JSON;
        let query_format_test = QueryFormat::NotSupported(String::from("test"));

        assert_eq!("geojson", query_format_geo_json.as_str());
        assert_eq!("json", query_format_json.as_str());
        assert_eq!("test", query_format_test.as_str());
    }

    #[test]
    fn query_format_from_str() {
        let geo_json = "geoJSON";
        let json = "JSON";
        let other = "PBF";
        let multi_geo_json = "JSON, PBF, geoJSON";
        let multi_json = "PBF, JSON";
        let multi_other = "PBF, KMZ";

        let query_format_geo_json = QueryFormat::from(geo_json);
        let query_format_json = QueryFormat::from(json);
        let query_format_other = QueryFormat::from(other);
        let query_format_multi_geo_json = QueryFormat::from(multi_geo_json);
        let query_format_multi_json = QueryFormat::from(multi_json);
        let query_format_multi_other = QueryFormat::from(multi_other);

        assert_eq!(QueryFormat::GeoJSON, query_format_geo_json);
        assert_eq!(QueryFormat::JSON, query_format_json);
        assert_eq!(QueryFormat::NotSupported(other.to_lowercase()), query_format_other);
        assert_eq!(QueryFormat::GeoJSON, query_format_multi_geo_json);
        assert_eq!(QueryFormat::JSON, query_format_multi_json);
        assert_eq!(QueryFormat::NotSupported(other.to_lowercase()), query_format_multi_other);
    }
}
