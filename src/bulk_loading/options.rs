use serde::{Deserialize, Serialize};

pub trait DataFileOptions : Serialize + for<'a> Deserialize<'a> + Sized {
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
