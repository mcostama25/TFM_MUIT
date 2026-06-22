mod sender;
pub use sender::HttpSender;

pub enum HttpAction {
    UpsertCatalog { urn: String, title: String },
    UpsertDataset { urn: String },
    DeleteCatalog { urn: String },
    DeleteDataset { urn: String },
    PatchPublisher { urn: String, publisher: String },
    PatchSchema { urn: String, fields: Vec<String> },
}
