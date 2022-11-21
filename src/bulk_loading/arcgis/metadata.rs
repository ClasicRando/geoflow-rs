use itertools::Itertools;
use reqwest::Url;
use serde::Deserialize;
use serde_aux::field_attributes::deserialize_string_from_number;
use serde_json::json;
use std::collections::HashMap;

use crate::bulk_loading::{
    analyze::{ColumnMetadata, ColumnType, Schema},
    error::{BulkDataError, BulkDataResult},
};

use super::scraping::QueryFormat;

#[derive(Deserialize)]
enum RestServiceGeometryType {
    #[serde(alias = "esriGeometryPoint")]
    Point,
    #[serde(alias = "esriGeometryMultipoint")]
    Multipoint,
    #[serde(alias = "esriGeometryPolyline")]
    Polyline,
    #[serde(alias = "esriGeometryPolygon")]
    Polygon,
    #[serde(alias = "esriGeometryEnvelope")]
    Envelope,
}

impl RestServiceGeometryType {
    fn name(&self) -> &'static str {
        match self {
            Self::Point => "esriGeometryPoint",
            Self::Multipoint => "esriGeometryMultipoint",
            Self::Polyline => "esriGeometryPolyline",
            Self::Polygon => "esriGeometryPolygon",
            Self::Envelope => "esriGeometryEnvelope",
        }
    }
}

#[derive(Clone, PartialEq, Deserialize)]
pub enum RestServiceFieldType {
    #[serde(alias = "esriFieldTypeBlob")]
    Blob,
    #[serde(alias = "esriFieldTypeDate")]
    Date,
    #[serde(alias = "esriFieldTypeDouble")]
    Double,
    #[serde(alias = "esriFieldTypeFloat")]
    Float,
    #[serde(alias = "esriFieldTypeGeometry")]
    Geometry,
    #[serde(alias = "esriFieldTypeGlobalID")]
    GlobalID,
    #[serde(alias = "esriFieldTypeGUID")]
    GUID,
    #[serde(alias = "esriFieldTypeInteger")]
    Integer,
    #[serde(alias = "esriFieldTypeOID")]
    OID,
    #[serde(alias = "esriFieldTypeRaster")]
    Raster,
    #[serde(alias = "esriFieldTypeSingle")]
    Single,
    #[serde(alias = "esriFieldTypeSmallInteger")]
    SmallInteger,
    #[serde(alias = "esriFieldTypeString")]
    String,
    #[serde(alias = "esriFieldTypeXML")]
    XML,
}

impl TryFrom<&RestServiceFieldType> for ColumnType {
    type Error = BulkDataError;

    fn try_from(value: &RestServiceFieldType) -> Result<Self, Self::Error> {
        Ok(match value {
            RestServiceFieldType::Blob => return Err("Blob type fields are not supported".into()),
            RestServiceFieldType::Date => ColumnType::Date,
            RestServiceFieldType::Double => ColumnType::DoublePrecision,
            RestServiceFieldType::Float => ColumnType::Real,
            RestServiceFieldType::Geometry => {
                return Err("Geometry type fields are not supported".into())
            }
            RestServiceFieldType::GlobalID => ColumnType::UUID,
            RestServiceFieldType::GUID => ColumnType::UUID,
            RestServiceFieldType::Integer => ColumnType::Integer,
            RestServiceFieldType::OID => ColumnType::Integer,
            RestServiceFieldType::Raster => {
                return Err("Raster type fields are not supported".into())
            }
            RestServiceFieldType::Single => ColumnType::Real,
            RestServiceFieldType::SmallInteger => ColumnType::SmallInt,
            RestServiceFieldType::String => ColumnType::Text,
            RestServiceFieldType::XML => ColumnType::Text,
        })
    }
}

#[derive(Deserialize)]
pub struct CodedValue {
    pub(crate) name: String,
    #[serde(deserialize_with = "deserialize_string_from_number")]
    pub(crate) code: String,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum FieldDomain {
    Range {
        name: String,
        range: Vec<i32>,
    },
    #[serde(alias = "codedValue")]
    Coded {
        #[serde(alias = "codedValues")]
        coded_values: Vec<CodedValue>,
    },
    Inherited,
}

fn coded_to_map(coded_values: &Vec<CodedValue>) -> HashMap<String, String> {
    coded_values
        .iter()
        .map(|coded_value| (coded_value.code.to_owned(), coded_value.name.to_owned()))
        .collect()
}

#[derive(Deserialize)]
pub struct ServiceField {
    name: String,
    #[serde(alias = "type")]
    field_type: RestServiceFieldType,
    domain: Option<FieldDomain>,
}

impl ServiceField {
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn field_type(&self) -> &RestServiceFieldType {
        &self.field_type
    }
}

impl ServiceField {
    pub fn is_coded(&self) -> Option<HashMap<String, String>> {
        if let Some(domain) = &self.domain {
            if let FieldDomain::Coded { coded_values, .. } = domain {
                Some(coded_to_map(coded_values))
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[derive(Deserialize)]
pub struct ArcGisRestJsonMetadata {
    name: String,
    #[serde(alias = "maxRecordCount")]
    max_record_count: i32,
    #[serde(alias = "type")]
    server_type: String,
    #[serde(alias = "geometryType")]
    geo_type: Option<RestServiceGeometryType>,
    fields: Vec<ServiceField>,
    #[serde(alias = "objectIdField")]
    oid_field: Option<String>,
    #[serde(alias = "supportedQueryFormats")]
    query_formats: String,
    #[serde(alias = "supportsPagination")]
    supports_pagination: Option<bool>,
    #[serde(alias = "supportsStatistics")]
    supports_statistics: Option<bool>,
    #[serde(alias = "advancedQueryCapabilities")]
    advanced_query_capabilities: Option<HashMap<String, bool>>,
}

impl ArcGisRestJsonMetadata {
    fn supports_pagination(&self) -> bool {
        self.supports_pagination.unwrap_or(false)
            || *self
                .advanced_query_capabilities
                .as_ref()
                .and_then(|aqc| aqc.get("supportsPagination"))
                .unwrap_or(&false)
    }

    fn supports_statistics(&self) -> bool {
        self.supports_statistics.unwrap_or(false)
            || *self
                .advanced_query_capabilities
                .as_ref()
                .and_then(|aqc| aqc.get("supportsStatistics"))
                .unwrap_or(&false)
    }
}

pub enum QueryIterator<'m> {
    OID {
        query_url: Url,
        oid_field_name: &'m str,
        min_oid: i32,
        scrape_count: i32,
        fields: String,
        url_params: Vec<(&'m str, &'m str)>,
        remaining_records_count: i32,
        query_index: i32,
    },
    Pagination {
        query_url: Url,
        scrape_count: i32,
        result_count: String,
        fields: String,
        url_params: Vec<(&'m str, &'m str)>,
        remaining_records_count: i32,
        query_index: i32,
    },
}

impl<'m> QueryIterator<'m> {
    fn new<'u>(metadata: &'m ArcGisRestMetadata<'u>) -> BulkDataResult<Self> {
        let scrape_count = metadata.scrape_count();
        let mut url_params = metadata.geometry_options()?;
        let fields = metadata.fields().map(|f| f.name.to_owned()).join(",");
        url_params.push(("f", metadata.query_format.as_str()));
        let query_url = match metadata.url.join("/query") {
            Ok(q) => q,
            Err(error) => return Err(error.into()),
        };
        if metadata.supports_pagination() {
            Ok(Self::Pagination {
                query_url,
                scrape_count,
                result_count: scrape_count.to_string(),
                fields,
                url_params,
                remaining_records_count: metadata.source_count,
                query_index: 0,
            })
        } else {
            let Some(ref oid_field_name) = metadata.json_metadata.oid_field else {
                return Err("OID is not found but OID queries are required".into())
            };
            let Some((_, min_oid)) = metadata.max_min_oid else {
                return Err("Min OID is not found but the value is required for scraping".into())
            };
            Ok(Self::OID {
                query_url,
                oid_field_name: &oid_field_name,
                min_oid,
                scrape_count,
                fields,
                url_params,
                remaining_records_count: metadata.source_count,
                query_index: 0,
            })
        }
    }

    #[inline]
    fn query_url(&self) -> &Url {
        match self {
            Self::OID { query_url, .. } => query_url,
            Self::Pagination { query_url, .. } => query_url,
        }
    }

    #[inline]
    fn remaining_records_count(&self) -> &i32 {
        match self {
            Self::OID {
                remaining_records_count,
                ..
            } => remaining_records_count,
            Self::Pagination {
                remaining_records_count,
                ..
            } => remaining_records_count,
        }
    }

    #[inline]
    fn zero_remaining_records_count(&mut self) {
        match self {
            Self::OID {
                remaining_records_count,
                ..
            } => *remaining_records_count = 0,
            Self::Pagination {
                remaining_records_count,
                ..
            } => *remaining_records_count = 0,
        }
    }

    #[inline]
    fn update_remaining_records_count(&mut self) {
        match self {
            Self::OID {
                remaining_records_count,
                ref scrape_count,
                ..
            } => *remaining_records_count -= scrape_count,
            Self::Pagination {
                remaining_records_count,
                ref scrape_count,
                ..
            } => *remaining_records_count -= scrape_count,
        }
    }

    #[inline]
    fn update_query_index(&mut self) {
        match self {
            Self::OID { query_index, .. } => *query_index += 1,
            Self::Pagination { query_index, .. } => *query_index += 1,
        }
    }

    #[inline]
    fn url_params(&self) -> &[(&str, &str)] {
        match self {
            Self::OID { url_params, .. } => url_params,
            Self::Pagination { url_params, .. } => url_params,
        }
    }

    #[inline]
    fn fields(&self) -> &str {
        match self {
            Self::OID { fields, .. } => fields,
            Self::Pagination { fields, .. } => fields,
        }
    }
}

impl<'m> Iterator for QueryIterator<'m> {
    type Item = BulkDataResult<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if *self.remaining_records_count() <= 0 {
            return None;
        }
        let mut url_params = self.url_params().to_vec();
        url_params.push(("outFields", self.fields()));
        let url_parse = match self {
            Self::OID {
                ref oid_field_name,
                ref min_oid,
                ref scrape_count,
                ref query_index,
                ..
            } => {
                let lower_bound = *min_oid + (*query_index * *scrape_count);
                let where_clause = format!(
                    "{} >= {} and {} <= {}",
                    oid_field_name,
                    lower_bound,
                    oid_field_name,
                    lower_bound + *scrape_count - 1,
                );
                url_params.push(("where", &where_clause));
                Url::parse_with_params(self.query_url().as_str(), url_params)
            }
            Self::Pagination {
                ref scrape_count,
                ref result_count,
                ref query_index,
                ..
            } => {
                let result_offset = format!("{}", *query_index * *scrape_count);
                url_params.push(("resultOffset", &result_offset));
                url_params.push(("resultRecordCount", &result_count));
                Url::parse_with_params(self.query_url().as_str(), url_params)
            }
        };
        let url = match url_parse {
            Ok(url) => url,
            Err(error) => {
                self.zero_remaining_records_count();
                return Some(Err(error.into()));
            }
        };
        self.update_query_index();
        self.update_remaining_records_count();
        Some(Ok(url.to_string()))
    }
}

pub struct ArcGisRestMetadata<'u> {
    url: &'u Url,
    json_metadata: ArcGisRestJsonMetadata,
    query_format: QueryFormat,
    source_count: i32,
    max_min_oid: Option<(i32, i32)>,
}

impl<'u> ArcGisRestMetadata<'u> {
    #[inline]
    pub fn name(&self) -> &str {
        &self.json_metadata.name
    }

    #[inline]
    fn server_type(&self) -> &str {
        &self.json_metadata.server_type
    }

    #[inline]
    fn is_table(&self) -> bool {
        self.server_type().to_uppercase() == "TABLE"
    }

    #[inline]
    fn supports_pagination(&self) -> bool {
        self.json_metadata.supports_pagination()
    }

    #[inline]
    fn scrape_count(&self) -> i32 {
        if self.json_metadata.max_record_count <= 10000 {
            self.json_metadata.max_record_count
        } else {
            10000
        }
    }

    #[inline]
    pub fn query_format(&self) -> &QueryFormat {
        &self.query_format
    }

    fn valid_service(&self) -> bool {
        self.supports_pagination() || self.json_metadata.oid_field.is_some()
    }

    pub fn fields(&self) -> impl Iterator<Item = &ServiceField> {
        self.json_metadata
            .fields
            .iter()
            .filter(|f| f.name != "Shape" && f.field_type != RestServiceFieldType::Geometry)
    }

    fn geometry_options(&self) -> BulkDataResult<Vec<(&str, &str)>> {
        if self.is_table() {
            Ok(vec![])
        } else {
            let Some(ref geometry_type) = self.json_metadata.geo_type else {
                return Err("Service is not a Table but geometry type unavailable".into())
            };
            Ok(vec![
                ("geometryType", geometry_type.name()),
                ("outSR", "4269"),
            ])
        }
    }

    pub fn queries(&self) -> BulkDataResult<QueryIterator> {
        if !self.valid_service() {
            return Err("Service is not valid for scraping. This means either the pagination option is not provided or there is no OID field".into());
        }
        QueryIterator::new(self)
    }

    pub async fn from_url(url: &'u Url) -> BulkDataResult<ArcGisRestMetadata<'u>> {
        let client = reqwest::Client::new();
        let source_count = get_service_count(&client, url).await?;
        let mut json_metadata = get_service_metadata(&client, url).await?;
        let mut oid_field = json_metadata.oid_field.take();

        if oid_field.is_none() {
            oid_field = json_metadata
                .fields
                .iter()
                .find(|field| field.field_type == RestServiceFieldType::OID)
                .map(|field| field.name.to_owned());
        }

        let max_min_oid = match oid_field {
            Some(oid) => {
                let max_min = if !json_metadata.supports_pagination() {
                    get_service_max_min(&client, url, &oid, json_metadata.supports_statistics())
                        .await?
                } else {
                    None
                };
                json_metadata.oid_field = Some(oid);
                max_min
            }
            None => None,
        };

        let format = QueryFormat::from(json_metadata.query_formats.as_str());

        let rest_metadata = Self {
            url,
            json_metadata: json_metadata,
            query_format: format,
            source_count: source_count.count,
            max_min_oid,
        };
        Ok(rest_metadata)
    }
}

impl<'u> TryFrom<ArcGisRestMetadata<'u>> for Schema {
    type Error = BulkDataError;

    fn try_from(value: ArcGisRestMetadata<'u>) -> Result<Self, Self::Error> {
        let columns: Vec<ColumnMetadata> = value
            .fields()
            .enumerate()
            .map(|(i, f)| -> BulkDataResult<ColumnMetadata> {
                ColumnMetadata::new(f.name(), i, f.field_type().try_into()?)
            })
            .collect::<BulkDataResult<_>>()?;
        Ok(Schema::new(value.name(), columns)?)
    }
}

#[derive(Deserialize)]
struct CountQueryResponse {
    count: i32,
}

async fn get_service_count(
    client: &reqwest::Client,
    url: &Url,
) -> BulkDataResult<CountQueryResponse> {
    let count_url = Url::parse_with_params(
        url.join("/query")?.as_str(),
        [("where", "1=1"), ("returnCountOnly", "true"), ("f", "json")],
    )?;
    let count_json: CountQueryResponse = client.get(count_url).send().await?.json().await?;
    Ok(count_json)
}

async fn get_service_metadata(
    client: &reqwest::Client,
    url: &Url,
) -> BulkDataResult<ArcGisRestJsonMetadata> {
    let metadata_url = Url::parse_with_params(url.as_str(), [("f", "json")])?;
    let metadata_json: ArcGisRestJsonMetadata =
        client.get(metadata_url).send().await?.json().await?;
    Ok(metadata_json)
}

#[derive(Deserialize)]
struct StatisticsResponseAttributes {
    #[serde(alias = "MAX_VALUE")]
    max: i32,
    #[serde(alias = "MIN_VALUE")]
    min: i32,
}

#[derive(Deserialize)]
struct StatisticsResponseFeature {
    attributes: StatisticsResponseAttributes,
}

#[derive(Deserialize)]
struct StatisticsResponse {
    features: Vec<StatisticsResponseFeature>,
}

fn out_statistics_parameter(oid_field_name: &str) -> String {
    json!([
        {
            "statisticType": "max",
            "onStatisticField": oid_field_name,
            "outStatisticFieldName": "MAX_VALUE",
        },
        {
            "statisticType": "min",
            "onStatisticField": oid_field_name,
            "outStatisticFieldName": "MIN_VALUE",
        },
    ])
    .to_string()
}

async fn get_service_max_min(
    client: &reqwest::Client,
    url: &Url,
    oid_field_name: &str,
    stats_enabled: bool,
) -> BulkDataResult<Option<(i32, i32)>> {
    let result = if stats_enabled {
        get_service_max_min_stats(&client, url, oid_field_name).await?
    } else {
        get_service_max_min_oid(&client, url).await?
    };
    Ok(result)
}

#[derive(Deserialize)]
struct ObjectIdsResponse {
    #[serde(alias = "objectIds")]
    object_ids: Vec<i32>,
}

async fn get_object_ids_response(
    client: &reqwest::Client,
    url: &Url,
) -> BulkDataResult<ObjectIdsResponse> {
    let max_min_url = Url::parse_with_params(
        url.join("/query")?.as_str(),
        [("where", "1=1"), ("returnIdsOnly", "true"), ("f", "json")],
    )?;
    let max_min_json = client.get(max_min_url).send().await?.json().await?;
    return Ok(max_min_json);
}

async fn get_service_max_min_oid(
    client: &reqwest::Client,
    url: &Url,
) -> BulkDataResult<Option<(i32, i32)>> {
    let max_min_json = get_object_ids_response(client, url).await?;
    Ok(Some((
        max_min_json.object_ids[max_min_json.object_ids.len() - 1],
        max_min_json.object_ids[0],
    )))
}

async fn get_service_max_min_stats(
    client: &reqwest::Client,
    url: &Url,
    oid_field_name: &str,
) -> BulkDataResult<Option<(i32, i32)>> {
    let out_statistics = out_statistics_parameter(oid_field_name);
    let max_min_url = Url::parse_with_params(
        url.join("/query")?.as_str(),
        [("outStatistics", out_statistics.as_str()), ("f", "json")],
    )?;
    let max_min_json: StatisticsResponse = client.get(max_min_url).send().await?.json().await?;
    if max_min_json.features.is_empty() {
        return Err("No features in max min response".into());
    }
    let feature = &max_min_json.features[0];
    Ok(Some((feature.attributes.max, feature.attributes.min)))
}
