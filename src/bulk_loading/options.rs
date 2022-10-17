use std::path::Path;

pub trait DataFileOptions {
    fn file_path(&self) -> &Path;
    fn delimiter(&self) -> &char {
        &','
    }
    fn header(&self) -> &bool {
        &false
    }
    fn qualified(&self) -> &bool {
        &true
    }
}

pub struct DelimitedDataOptions<'p> {
    file_path: &'p Path,
    delimiter: char,
    qualified: bool,
}

impl<'p> DataFileOptions for DelimitedDataOptions<'p> {
    #[inline]
    fn file_path(&self) -> &Path {
        self.file_path
    }

    #[inline]
    fn delimiter(&self) -> &char {
        &self.delimiter
    }

    #[inline]
    fn header(&self) -> &bool {
        &true
    }

    #[inline]
    fn qualified(&self) -> &bool {
        &self.qualified
    }
}

pub struct ExcelOptions<'p> {
    file_path: &'p Path,
    sheet_name: String,
}

impl<'p> ExcelOptions<'p> {
    #[inline]
    pub fn sheet_name(&self) -> &str {
        &self.sheet_name
    }
}

impl<'p> DataFileOptions for ExcelOptions<'p> {
    #[inline]
    fn file_path(&self) -> &Path {
        self.file_path
    }
}

pub struct DefaultFileOptions<'p> {
    file_path: &'p Path,
}

impl<'p> DataFileOptions for DefaultFileOptions<'p> {
    #[inline]
    fn file_path(&self) -> &Path {
        self.file_path
    }
}
