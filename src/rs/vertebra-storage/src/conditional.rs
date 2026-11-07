//! Provider identity and atomic multipart publication.

use std::sync::Arc;

use async_trait::async_trait;
use object_store::aws::AmazonS3;
use object_store::azure::MicrosoftAzure;
use object_store::gcp::GoogleCloudStorage;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, PutResult,
    UpdateVersion, UploadPart,
};

use crate::oci::OciObjectStore;
use crate::store::{ObjectStoreError, PutConditionalOptions, StorageError, build_store_attributes};

#[derive(Clone, Debug)]
pub(crate) enum PublishCondition {
    Overwrite,
    Create,
    Match(String),
    NotMatch(String),
}

impl PublishCondition {
    pub(crate) fn normalize(options: &PutConditionalOptions) -> Result<Self, ObjectStoreError> {
        if usize::from(options.if_not_exists)
            + usize::from(options.if_match.is_some())
            + usize::from(options.if_none_match.is_some())
            > 1
        {
            return Err(StorageError::InvalidInput(
                "publication selectors are mutually exclusive".into(),
            ));
        }
        Ok(match (&options.if_match, &options.if_none_match) {
            (Some(tag), _) => Self::Match(tag.clone()),
            (_, Some(tag)) if tag != "*" => Self::NotMatch(tag.clone()),
            (_, Some(_)) => Self::Create,
            _ if options.if_not_exists => Self::Create,
            _ => Self::Overwrite,
        })
    }
}

#[derive(Clone)]
pub(crate) enum PublicationBackend {
    Memory(Arc<InMemory>),
    Local(Arc<LocalFileSystem>),
    S3(Arc<AmazonS3>),
    Azure(Arc<MicrosoftAzure>),
    Gcs(Arc<GoogleCloudStorage>),
    Oci(Arc<OciObjectStore>),
    Custom(Arc<dyn ObjectStore>),
}

impl PublicationBackend {
    pub(crate) async fn begin(
        &self,
        path: &Path,
        options: &PutConditionalOptions,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError> {
        let condition = PublishCondition::normalize(options)?;
        if matches!(condition, PublishCondition::NotMatch(_)) {
            return Err(object_store::Error::NotSupported {
                source: "negative multipart publication requires atomic predicate support".into(),
            }
            .into());
        }
        if let Self::Memory(store) = self {
            return Ok(Box::new(MemoryPublication {
                store: store.clone(),
                path: path.clone(),
                condition,
                attributes: build_store_attributes(
                    options.content_type.as_deref(),
                    &options.attributes,
                ),
                parts: Vec::new(),
            }));
        }
        if !matches!(condition, PublishCondition::Overwrite) {
            return Err(object_store::Error::NotSupported {
                source: "conditional multipart publication is unavailable for this backend".into(),
            }
            .into());
        }
        let store: &dyn ObjectStore = match self {
            Self::Memory(store) => store.as_ref(),
            Self::Local(store) => store.as_ref(),
            Self::S3(store) => store.as_ref(),
            Self::Azure(store) => store.as_ref(),
            Self::Gcs(store) => store.as_ref(),
            Self::Oci(store) => store.as_ref(),
            Self::Custom(store) => store.as_ref(),
        };
        let result = store
            .put_multipart_opts(
                path,
                object_store::PutMultipartOptions {
                    attributes: build_store_attributes(
                        options.content_type.as_deref(),
                        &options.attributes,
                    ),
                    ..Default::default()
                },
            )
            .await;
        match result {
            Err(object_store::Error::NotImplemented { .. }) if matches!(self, Self::Local(_)) => {
                Ok(store.put_multipart(path).await?)
            }
            other => Ok(other?),
        }
    }
}

#[derive(Debug)]
struct MemoryPublication {
    store: Arc<InMemory>,
    path: Path,
    condition: PublishCondition,
    attributes: object_store::Attributes,
    parts: Vec<PutPayload>,
}

#[async_trait]
impl MultipartUpload for MemoryPublication {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        self.parts.push(payload);
        Box::pin(std::future::ready(Ok(())))
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mode = match &self.condition {
            PublishCondition::Overwrite => PutMode::Overwrite,
            PublishCondition::Create => PutMode::Create,
            PublishCondition::Match(tag) => PutMode::Update(UpdateVersion {
                e_tag: Some(tag.clone()),
                version: None,
            }),
            PublishCondition::NotMatch(_) => {
                return Err(object_store::Error::NotSupported {
                    source: "negative multipart predicate is unavailable".into(),
                });
            }
        };
        self.store
            .put_opts(
                &self.path,
                self.parts.iter().flatten().cloned().collect(),
                PutOptions {
                    mode,
                    attributes: self.attributes.clone(),
                    ..Default::default()
                },
            )
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.clear();
        Ok(())
    }
}
