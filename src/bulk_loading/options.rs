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
