use polars::prelude::PolarsError;
use std::fmt::Display;

pub type BulkDataResult<T> = Result<T, BulkDataError>;

#[derive(Debug)]
pub enum BulkDataError {
    Generic(String),
    Polars(PolarsError),
    SQL(sqlx::Error),
    Fmt(std::fmt::Error),
    IO(std::io::Error),
    Excel(calamine::Error),
    Shp(shapefile::Error),
    GeoJSON(geojson::Error),
    Parquet(parquet::errors::ParquetError),
    Wkb(wkb::WKBReadError),
}

impl std::error::Error for BulkDataError {}

impl Display for BulkDataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Generic(string) => write!(f, "Loader Error\n{}", string),
            Self::Polars(error) => write!(f, "Polars Error\n{}", error),
            Self::SQL(error) => write!(f, "Polars Error\n{}", error),
            Self::Fmt(error) => write!(f, "Format Error\n{}", error),
            Self::IO(error) => write!(f, "IO Error\n{}", error),
            Self::Excel(error) => write!(f, "Excel Error\n{}", error),
            Self::Shp(error) => write!(f, "Shapefile Error\n{}", error),
            Self::GeoJSON(error) => write!(f, "GeoJSON Error\n{}", error),
            Self::Parquet(error) => write!(f, "Parquet Error\n{}", error),
            Self::Wkb(error) => write!(f, "WKB Error\n{:?}", error),
        }
    }
}

impl From<PolarsError> for BulkDataError {
    fn from(error: PolarsError) -> Self {
        Self::Polars(error)
    }
}

impl From<sqlx::Error> for BulkDataError {
    fn from(error: sqlx::Error) -> Self {
        Self::SQL(error)
    }
}

impl From<std::fmt::Error> for BulkDataError {
    fn from(error: std::fmt::Error) -> Self {
        Self::Fmt(error)
    }
}

impl From<std::io::Error> for BulkDataError {
    fn from(error: std::io::Error) -> Self {
        Self::IO(error)
    }
}

impl From<calamine::Error> for BulkDataError {
    fn from(error: calamine::Error) -> Self {
        Self::Excel(error)
    }
}

impl From<&str> for BulkDataError {
    fn from(error: &str) -> Self {
        Self::Generic(error.to_owned())
    }
}

impl From<String> for BulkDataError {
    fn from(error: String) -> Self {
        Self::Generic(error)
    }
}

impl From<shapefile::Error> for BulkDataError {
    fn from(error: shapefile::Error) -> Self {
        Self::Shp(error)
    }
}

impl From<shapefile::dbase::Error> for BulkDataError {
    fn from(error: shapefile::dbase::Error) -> Self {
        Self::Shp(shapefile::Error::DbaseError(error))
    }
}

impl From<geojson::Error> for BulkDataError {
    fn from(error: geojson::Error) -> Self {
        Self::GeoJSON(error)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for BulkDataError {
    fn from(error: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::Generic(format!("{}", error))
    }
}

impl From<parquet::errors::ParquetError> for BulkDataError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Parquet(error)
    }
}

impl From<wkb::WKBReadError> for BulkDataError {
    fn from(error: wkb::WKBReadError) -> Self {
        Self::Wkb(error)
    }
}

