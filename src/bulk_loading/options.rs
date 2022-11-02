use std::path::PathBuf;

pub trait DataFileOptions {
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

pub struct DefaultFileOptions {
    pub file_path: PathBuf,
}

impl DefaultFileOptions {
    pub fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }
}

impl DataFileOptions for DefaultFileOptions {}
