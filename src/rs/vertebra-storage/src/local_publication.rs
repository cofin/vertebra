use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::{ObjectStoreExt, PutResult};
use tokio::io::AsyncWriteExt;

use crate::conditional::PublishCondition;
use crate::store::StorageError;

const LOCK_NAME: &str = ".vtb-cas-lock#0";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DirectoryIdentity(u64, u128);

#[cfg(unix)]
fn directory_identity(path: &Path) -> io::Result<DirectoryIdentity> {
    use std::os::unix::fs::MetadataExt;
    let metadata = File::open(path)?.metadata()?;
    Ok(DirectoryIdentity(
        metadata.dev(),
        u128::from(metadata.ino()),
    ))
}

#[cfg(windows)]
fn directory_identity(path: &Path) -> io::Result<DirectoryIdentity> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
    };

    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    // SAFETY: FILE_ID_INFO contains only integer fields and a byte array, all valid when zeroed.
    let mut info: FILE_ID_INFO = unsafe { std::mem::zeroed() };
    // SAFETY: directory owns a live handle; info is writable and has the declared size.
    let result = unsafe {
        GetFileInformationByHandleEx(
            directory.as_raw_handle(),
            FileIdInfo,
            std::ptr::from_mut(&mut info).cast(),
            std::mem::size_of::<FILE_ID_INFO>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(DirectoryIdentity(
        info.VolumeSerialNumber,
        u128::from_ne_bytes(info.FileId.Identifier),
    ))
}

fn open_lock(parent: &Path) -> io::Result<File> {
    let path = parent.join(LOCK_NAME);
    match File::open(&path) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => Ok(file),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => File::open(path),
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

pub(super) struct DirectoryGuard {
    file: File,
}

impl Drop for DirectoryGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub(super) struct LocalPublication {
    pub(super) store: Arc<LocalFileSystem>,
    root: PathBuf,
}

pub(super) struct LocalStage {
    path: Option<PathBuf>,
    file: Option<tokio::fs::File>,
}

impl LocalStage {
    pub(super) async fn write_chunk(&mut self, chunk: Bytes) -> Result<(), StorageError> {
        self.file
            .as_mut()
            .ok_or_else(|| StorageError::InvalidInput("local stage is closed".into()))?
            .write_all(&chunk)
            .await?;
        Ok(())
    }

    pub(super) async fn abort(&mut self) -> Result<(), StorageError> {
        self.file.take();
        if let Some(path) = &self.path {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            self.path = None;
        }
        Ok(())
    }
}

impl Drop for LocalStage {
    fn drop(&mut self) {
        self.file.take();
        if let Some(path) = &self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl LocalPublication {
    pub(super) fn new(store: Arc<LocalFileSystem>, root: PathBuf) -> Self {
        Self { store, root }
    }

    pub(super) async fn stage(&self, destination: &StorePath) -> Result<LocalStage, StorageError> {
        let destination = self.store.path_to_filesystem(destination)?;
        let parent = destination.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "destination has no parent")
        })?;
        std::fs::create_dir_all(parent)?;
        for counter in 0_u64.. {
            let path = parent.join(format!(
                ".vertebra-stage-{}#{counter}",
                uuid::Uuid::new_v4()
            ));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    return Ok(LocalStage {
                        path: Some(path),
                        file: Some(tokio::fs::File::from_std(file)),
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(io::Error::other("local stage names exhausted").into())
    }

    pub(super) async fn publish(
        &self,
        stage: &mut LocalStage,
        destination: &StorePath,
        condition: &PublishCondition,
    ) -> Result<PutResult, StorageError> {
        self.publish_with_parents(stage, destination, condition, &[])
            .await
    }

    async fn publish_with_parents(
        &self,
        stage: &mut LocalStage,
        destination: &StorePath,
        condition: &PublishCondition,
        source: &[PathBuf],
    ) -> Result<PutResult, StorageError> {
        stage
            .file
            .as_mut()
            .ok_or_else(|| StorageError::InvalidInput("local stage is closed".into()))?
            .flush()
            .await?;
        let path = stage
            .path
            .as_ref()
            .ok_or_else(|| StorageError::InvalidInput("local stage is closed".into()))?;
        let target = self.store.path_to_filesystem(destination)?;
        if path.parent().map(std::fs::canonicalize).transpose()?
            != target.parent().map(std::fs::canonicalize).transpose()?
        {
            return Err(StorageError::InvalidInput(
                "local stage has a different parent".into(),
            ));
        }
        let mut parents = source.to_vec();
        parents.push(target.clone());
        let _guards = self.lock_parents(&parents).await?;
        let current = match self.store.head(destination).await {
            Ok(meta) => Some(meta),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(error) => return Err(error.into()),
        };
        let matches = match condition {
            PublishCondition::Overwrite => true,
            PublishCondition::Create => current.is_none(),
            PublishCondition::Match(tag) => {
                current.as_ref().and_then(|meta| meta.e_tag.as_ref()) == Some(tag)
            }
            PublishCondition::NotMatch(tag) => {
                current.as_ref().and_then(|meta| meta.e_tag.as_ref()) != Some(tag)
            }
        };
        if !matches {
            return Err(if matches!(condition, PublishCondition::Create) {
                StorageError::AlreadyExists {
                    path: destination.to_string(),
                    message: "local destination exists".into(),
                }
            } else {
                StorageError::Precondition {
                    path: destination.to_string(),
                    message: "local ETag predicate failed".into(),
                }
            });
        }
        if matches!(condition, PublishCondition::Create) {
            std::fs::hard_link(path, &target).map_err(|error| {
                if error.kind() == io::ErrorKind::AlreadyExists {
                    StorageError::AlreadyExists {
                        path: destination.to_string(),
                        message: error.to_string(),
                    }
                } else {
                    error.into()
                }
            })?;
        } else {
            std::fs::rename(path, &target)?;
        }
        let receipt = self.store.head(destination).await?;
        stage.abort().await?;
        Ok(PutResult {
            e_tag: receipt.e_tag,
            version: receipt.version,
        })
    }

    pub(super) async fn delete(&self, path: &StorePath) -> Result<(), StorageError> {
        let target = self.store.path_to_filesystem(path)?;
        let _guards = self.lock_parents(std::slice::from_ref(&target)).await?;
        std::fs::remove_file(target)?;
        Ok(())
    }

    pub(super) async fn rename(
        &self,
        from: &StorePath,
        to: &StorePath,
        if_not_exists: bool,
    ) -> Result<(), StorageError> {
        let source = self.store.path_to_filesystem(from)?;
        let destination = self.store.path_to_filesystem(to)?;
        std::fs::create_dir_all(
            destination
                .parent()
                .ok_or_else(|| io::Error::other("destination has no parent"))?,
        )?;
        let _guards = self
            .lock_parents(&[source.clone(), destination.clone()])
            .await?;
        if if_not_exists {
            std::fs::hard_link(&source, &destination).map_err(|error| {
                if error.kind() == io::ErrorKind::AlreadyExists {
                    StorageError::AlreadyExists {
                        path: to.to_string(),
                        message: error.to_string(),
                    }
                } else {
                    error.into()
                }
            })?;
            std::fs::remove_file(source)?;
        } else {
            std::fs::rename(source, destination)?;
        }
        Ok(())
    }

    pub(super) async fn copy(&self, from: &StorePath, to: &StorePath) -> Result<(), StorageError> {
        let source = self.store.path_to_filesystem(from)?;
        let mut input = tokio::fs::File::open(&source).await?;
        let mut stage = self.stage(to).await?;
        tokio::io::copy(&mut input, stage.file.as_mut().expect("new stage is open")).await?;
        self.publish_with_parents(&mut stage, to, &PublishCondition::Overwrite, &[source])
            .await?;
        Ok(())
    }

    pub(super) async fn lock_parents(
        &self,
        paths: &[PathBuf],
    ) -> Result<Vec<DirectoryGuard>, StorageError> {
        let mut parents = Vec::with_capacity(paths.len());
        for path in paths {
            let destination = self.root.join(path);
            let parent = destination.parent().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "destination has no parent")
            })?;
            let parent = parent.canonicalize()?;
            parents.push((directory_identity(&parent)?, parent));
        }
        parents.sort_unstable_by_key(|(identity, _)| *identity);
        parents.dedup_by_key(|(identity, _)| *identity);
        let mut guards = Vec::with_capacity(parents.len());
        for (_, parent) in parents {
            let file = open_lock(&parent)?;
            loop {
                match file.try_lock() {
                    Ok(()) => break,
                    Err(std::fs::TryLockError::WouldBlock) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
                }
            }
            guards.push(DirectoryGuard { file });
        }
        Ok(guards)
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalPublication, directory_identity};
    use crate::conditional::PublishCondition;
    use crate::store::StorageError;
    use bytes::Bytes;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as StorePath;
    use object_store::{ObjectStore, ObjectStoreExt};
    use std::fs::{File, OpenOptions};
    use std::io::{BufRead, BufReader, Write};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;

    const LOCK_NAME: &str = ".vtb-cas-lock#0";

    #[cfg(unix)]
    #[tokio::test]
    async fn sdk_create_only_rename_partial_failure_characterization() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let source = StorePath::from("readonly/source");
        let destination = StorePath::from("writable/destination");
        store
            .put(&source, Bytes::from_static(b"source payload").into())
            .await
            .unwrap();
        std::fs::create_dir(root.path().join("writable")).unwrap();
        let parent = root.path().join("readonly");
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).unwrap();
        let result = store.rename_if_not_exists(&source, &destination).await;
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(result.is_err());
        for key in [&source, &destination] {
            assert_eq!(
                store.get(key).await.unwrap().bytes().await.unwrap(),
                b"source payload"[..]
            );
        }
    }

    #[tokio::test]
    async fn create_only_rename_preserves_conflicts_and_waits_for_parent_guards() {
        let root = tempfile::tempdir().unwrap();
        let publication = publication(root.path());
        let source = StorePath::from("from/source");
        let destination = StorePath::from("to/destination");
        publication
            .store
            .put(&source, Bytes::from_static(b"source").into())
            .await
            .unwrap();
        publication
            .store
            .put(&destination, Bytes::from_static(b"before").into())
            .await
            .unwrap();
        let held = open_lock(&root.path().join("from"));
        held.try_lock().unwrap();
        let mut waiting = Box::pin(publication.rename(&source, &destination, true));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        publication
            .store
            .put(
                &destination,
                Bytes::from_static(b"competing destination").into(),
            )
            .await
            .unwrap();
        drop(held);
        assert!(waiting.await.unwrap_err().is_already_exists());
        assert_eq!(
            publication
                .store
                .get(&source)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"source"[..]
        );
        assert_eq!(
            publication
                .store
                .get(&destination)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"competing destination"[..]
        );
        publication.delete(&destination).await.unwrap();
        publication
            .rename(&source, &destination, true)
            .await
            .unwrap();
        assert!(matches!(
            publication.store.head(&source).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(
            publication
                .store
                .get(&destination)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"source"[..]
        );
        publication
            .store
            .put(&source, Bytes::from_static(b"overwrite").into())
            .await
            .unwrap();
        publication
            .rename(&source, &destination, false)
            .await
            .unwrap();
        assert!(matches!(
            publication.store.head(&source).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(
            publication
                .store
                .get(&destination)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"overwrite"[..]
        );
        assert!(root.path().join("from").join(LOCK_NAME).exists());
        assert!(root.path().join("to").join(LOCK_NAME).exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_only_rename_reports_source_removal_failure_without_rollback() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let publication = publication(root.path());
        let source = StorePath::from("readonly/source");
        let destination = StorePath::from("writable/destination");
        publication
            .store
            .put(&source, Bytes::from_static(b"source payload").into())
            .await
            .unwrap();
        let parent = root.path().join("readonly");
        drop(open_lock(&parent));
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o555)).unwrap();
        let result = publication.rename(&source, &destination, true).await;
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            matches!(result, Err(StorageError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied)
        );
        for key in [&source, &destination] {
            assert_eq!(
                publication
                    .store
                    .get(key)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                b"source payload"[..]
            );
        }
        assert!(parent.join(LOCK_NAME).exists());
    }

    #[tokio::test]
    async fn stages_are_private_and_cleanup_only_owned_artifacts() {
        use futures::TryStreamExt;
        let root = tempfile::tempdir().unwrap();
        let publication = publication(root.path());
        let key = StorePath::from("nested/keep#suffix");
        let mut stage = publication.stage(&key).await.unwrap();
        stage.write_chunk(Bytes::from_static(b"one")).await.unwrap();
        stage.write_chunk(Bytes::from_static(b"two")).await.unwrap();
        stage.file.as_mut().unwrap().flush().await.unwrap();
        let path = stage.path.clone().unwrap();
        assert_eq!(path.parent().unwrap(), root.path().join("nested"));
        assert_eq!(std::fs::read(&path).unwrap(), b"onetwo");
        let mut other = publication.stage(&key).await.unwrap();
        let other_path = other.path.clone().unwrap();
        assert_ne!(path, other_path);
        std::fs::write(root.path().join("nested/keep#suffix"), b"kept").unwrap();
        drop(open_lock(&root.path().join("nested")));
        let narrow = LocalFileSystem::new_with_prefix(root.path().join("nested")).unwrap();
        for (store, prefix) in [
            (publication.store.as_ref(), Some(StorePath::from("nested"))),
            (&narrow, None),
        ] {
            let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
            assert_eq!(listed.len(), 1);
            assert!(listed[0].location.as_ref().ends_with("keep#suffix"));
            assert_eq!(
                store
                    .list_with_delimiter(prefix.as_ref())
                    .await
                    .unwrap()
                    .objects
                    .len(),
                1
            );
        }
        stage.abort().await.unwrap();
        stage.abort().await.unwrap();
        assert!(!path.exists());
        assert!(other_path.exists());
        assert!(root.path().join("nested").join(LOCK_NAME).exists());
        other.abort().await.unwrap();
        let abandoned = publication.stage(&key).await.unwrap();
        let abandoned_path = abandoned.path.clone().unwrap();
        drop(abandoned);
        assert!(!abandoned_path.exists());
        assert_eq!(
            std::fs::read(root.path().join("nested/keep#suffix")).unwrap(),
            b"kept"
        );
    }

    #[tokio::test]
    async fn native_publish_checks_current_predicates_and_returns_sdk_receipts() {
        let root = tempfile::tempdir().unwrap();
        let publication = publication(root.path());
        let key = StorePath::from("nested/%2F-λ");
        let mut stage = publication.stage(&key).await.unwrap();
        stage
            .write_chunk(Bytes::from_static(b"first"))
            .await
            .unwrap();
        let path = stage.path.clone().unwrap();
        let first = publication
            .publish(&mut stage, &key, &PublishCondition::Create)
            .await
            .unwrap();
        assert!(!path.exists());
        assert!(stage.path.is_none());
        assert_eq!(
            first.e_tag,
            publication.store.head(&key).await.unwrap().e_tag
        );
        let mut replacement = publication.stage(&key).await.unwrap();
        replacement
            .write_chunk(Bytes::from_static(b"replacement"))
            .await
            .unwrap();
        for condition in [
            PublishCondition::Create,
            PublishCondition::Match("stale".into()),
            PublishCondition::NotMatch(first.e_tag.clone().unwrap()),
        ] {
            let error = publication
                .publish(&mut replacement, &key, &condition)
                .await
                .unwrap_err();
            assert!(error.is_already_exists() || error.is_precondition());
            assert!(replacement.path.as_ref().unwrap().exists());
            assert_eq!(
                publication
                    .store
                    .get(&key)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                b"first"[..]
            );
        }
        let second = publication
            .publish(
                &mut replacement,
                &key,
                &PublishCondition::Match(first.e_tag.unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(
            second.e_tag,
            publication.store.head(&key).await.unwrap().e_tag
        );
        assert_eq!(
            second.version,
            publication.store.head(&key).await.unwrap().version
        );
        assert_eq!(
            publication
                .store
                .get(&key)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"replacement"[..]
        );
        let absent = StorePath::from("nested/absent");
        let mut stage = publication.stage(&absent).await.unwrap();
        assert!(
            publication
                .publish(
                    &mut stage,
                    &absent,
                    &PublishCondition::Match("missing".into())
                )
                .await
                .unwrap_err()
                .is_precondition()
        );
        publication
            .publish(
                &mut stage,
                &absent,
                &PublishCondition::NotMatch("missing".into()),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn competing_sdk_write_is_checked_after_parent_guard_acquisition() {
        let root = tempfile::tempdir().unwrap();
        let publication = publication(root.path());
        let key = StorePath::from("object");
        let before = publication
            .store
            .put(&key, Bytes::from_static(b"before").into())
            .await
            .unwrap();
        let mut stage = publication.stage(&key).await.unwrap();
        stage
            .write_chunk(Bytes::from_static(b"ours"))
            .await
            .unwrap();
        stage.file.as_mut().unwrap().flush().await.unwrap();
        let held = open_lock(root.path());
        held.try_lock().unwrap();
        let condition = PublishCondition::Match(before.e_tag.unwrap());
        let mut waiting = Box::pin(publication.publish(&mut stage, &key, &condition));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        let winner = publication
            .store
            .put(&key, Bytes::from_static(b"competing native SDK").into())
            .await
            .unwrap();
        drop(held);
        assert!(waiting.await.unwrap_err().is_precondition());
        assert_eq!(
            publication.store.head(&key).await.unwrap().e_tag,
            winner.e_tag
        );
        stage.abort().await.unwrap();
    }

    #[tokio::test]
    async fn native_publication_errors_retain_stage_and_original_destination() {
        let root = tempfile::tempdir().unwrap();
        let publication = publication(root.path());
        let key = StorePath::from("target");
        let mut stage = publication.stage(&key).await.unwrap();
        stage
            .write_chunk(Bytes::from_static(b"owned"))
            .await
            .unwrap();
        let path = stage.path.clone().unwrap();
        std::fs::create_dir(root.path().join("target")).unwrap();
        assert!(
            publication
                .publish(&mut stage, &key, &PublishCondition::Overwrite)
                .await
                .is_err()
        );
        assert!(path.exists());
        assert!(root.path().join("target").is_dir());
        std::fs::create_dir(root.path().join("elsewhere")).unwrap();
        assert!(matches!(
            publication
                .publish(
                    &mut stage,
                    &StorePath::from("elsewhere/object"),
                    &PublishCondition::Overwrite
                )
                .await,
            Err(StorageError::InvalidInput(_))
        ));
        assert!(path.exists());
        stage.abort().await.unwrap();
        assert!(!path.exists());
        assert!(root.path().join(LOCK_NAME).exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let denied = root.path().join("denied");
            std::fs::create_dir(&denied).unwrap();
            std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o555)).unwrap();
            let error = publication
                .stage(&StorePath::from("denied/object"))
                .await
                .err()
                .unwrap();
            std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o755)).unwrap();
            assert!(
                matches!(error, StorageError::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied)
            );
        }
    }

    #[tokio::test]
    async fn cancelled_publication_retains_abort_ownership_and_mutations_share_guards() {
        let root = tempfile::tempdir().unwrap();
        let publication = publication(root.path());
        let key = StorePath::from("source");
        publication
            .store
            .put(&key, Bytes::from_static(b"original").into())
            .await
            .unwrap();
        let mut stage = publication.stage(&key).await.unwrap();
        stage
            .write_chunk(Bytes::from_static(b"cancelled"))
            .await
            .unwrap();
        stage.file.as_mut().unwrap().flush().await.unwrap();
        let path = stage.path.clone().unwrap();
        let held = open_lock(root.path());
        held.try_lock().unwrap();
        let mut waiting =
            Box::pin(publication.publish(&mut stage, &key, &PublishCondition::Overwrite));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(waiting);
        assert!(path.exists());
        stage.abort().await.unwrap();
        stage.abort().await.unwrap();
        let mut deleting = Box::pin(publication.delete(&key));
        assert!(futures::poll!(deleting.as_mut()).is_pending());
        drop(deleting);
        assert_eq!(
            publication
                .store
                .get(&key)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"original"[..]
        );
        drop(held);
        let copied = StorePath::from("other/copy");
        publication.copy(&key, &copied).await.unwrap();
        let moved = StorePath::from("third/moved");
        publication.rename(&copied, &moved, false).await.unwrap();
        assert!(publication.store.head(&copied).await.is_err());
        assert_eq!(
            publication
                .store
                .get(&moved)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"original"[..]
        );
        publication.delete(&moved).await.unwrap();
        assert!(publication.store.head(&moved).await.is_err());
        assert!(root.path().join(LOCK_NAME).exists());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn same_parent_filesystem_and_sdk_characterization() {
        use futures::TryStreamExt;
        use object_store::path::Path as StorePath;
        use object_store::{ObjectStore, ObjectStoreExt};

        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("nested");
        std::fs::create_dir(&parent).unwrap();
        let broad = LocalFileSystem::new_with_prefix(root.path()).unwrap();
        let narrow = LocalFileSystem::new_with_prefix(&parent).unwrap();
        let stage = parent.join(format!(".vertebra-stage-{}#0", uuid::Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&stage)
            .unwrap();
        file.write_all(b"first").unwrap();
        file.write_all(b"second").unwrap();
        file.flush().unwrap();
        drop(file);
        drop(open_lock(&parent));
        std::fs::write(parent.join("keep#suffix"), b"legal").unwrap();
        for store in [&broad, &narrow] {
            let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
            assert_eq!(listed.len(), 1);
            assert!(listed[0].location.as_ref().ends_with("keep#suffix"));
        }
        assert_eq!(
            narrow
                .list_with_delimiter(None)
                .await
                .unwrap()
                .objects
                .len(),
            1
        );
        assert_eq!(
            broad
                .list_with_delimiter(Some(&StorePath::from("nested")))
                .await
                .unwrap()
                .objects
                .len(),
            1
        );
        let destination = narrow
            .path_to_filesystem(&StorePath::from("%2F-λ"))
            .unwrap();
        std::fs::hard_link(&stage, &destination).unwrap();
        assert_eq!(
            std::fs::hard_link(&stage, &destination).unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );
        let original = narrow.head(&StorePath::from("%2F-λ")).await.unwrap();
        assert_eq!(original.size, 11);
        assert_eq!(
            narrow
                .get(&StorePath::from("%2F-λ"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"firstsecond"[..]
        );
        std::fs::remove_file(&stage).unwrap();
        std::fs::write(&stage, b"replacement").unwrap();
        std::fs::rename(&stage, &destination).unwrap();
        assert!(!stage.exists());
        assert_eq!(
            narrow
                .get(&StorePath::from("%2F-λ"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"replacement"[..]
        );
    }

    fn open_lock(parent: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(parent.join(LOCK_NAME))
            .unwrap()
    }

    fn child(parent: &Path, mode: &str) -> Child {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "local_publication::tests::os_lock_child",
                "--nocapture",
            ])
            .env("VTB_LOCK_CHILD_PARENT", parent)
            .env("VTB_LOCK_CHILD_MODE", mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn marker(child: &mut Child, expected: &str) {
        let stdout = child.stdout.as_mut().unwrap();
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            assert_ne!(
                reader.read_line(&mut line).unwrap(),
                0,
                "missing {expected}"
            );
            if line.trim() == expected {
                return;
            }
        }
    }

    #[test]
    fn os_lock_child() {
        let Some(parent) = std::env::var_os("VTB_LOCK_CHILD_PARENT") else {
            return;
        };
        let file = open_lock(Path::new(&parent));
        match std::env::var("VTB_LOCK_CHILD_MODE").unwrap().as_str() {
            "hold" => {
                file.try_lock().unwrap();
                println!("LOCKED");
                std::io::stdout().flush().unwrap();
                let mut release = String::new();
                std::io::stdin().read_line(&mut release).unwrap();
            }
            "probe" => {
                assert!(matches!(
                    file.try_lock(),
                    Err(std::fs::TryLockError::WouldBlock)
                ));
                println!("CONTENDED");
            }
            "primitive" => {
                let publication = publication(Path::new(&parent));
                let paths = ["object".into()];
                let guards =
                    crate::store::block_on_storage(publication.lock_parents(&paths)).unwrap();
                println!("LOCKED");
                std::io::stdout().flush().unwrap();
                let mut release = String::new();
                std::io::stdin().read_line(&mut release).unwrap();
                drop(guards);
            }
            other => panic!("unknown child mode {other}"),
        }
    }

    #[test]
    fn os_lock_characterization() {
        let temp = tempfile::tempdir().unwrap();
        let file = open_lock(temp.path());
        file.try_lock().unwrap();
        let mut contender = child(temp.path(), "probe");
        marker(&mut contender, "CONTENDED");
        assert!(contender.wait().unwrap().success());
        drop(file);
        open_lock(temp.path()).try_lock().unwrap();
        assert!(temp.path().join(LOCK_NAME).exists());
    }

    #[cfg(unix)]
    #[test]
    fn os_directory_alias_characterization() {
        use std::os::unix::fs::{MetadataExt, symlink};
        let temp = tempfile::tempdir().unwrap();
        let real = temp.path().join("real");
        let alias = temp.path().join("alias");
        std::fs::create_dir(&real).unwrap();
        symlink(&real, &alias).unwrap();
        let physical = std::fs::metadata(&real).unwrap();
        let alternate = std::fs::metadata(&alias).unwrap();
        assert_eq!(
            (physical.dev(), physical.ino()),
            (alternate.dev(), alternate.ino())
        );
        let file = open_lock(&real);
        file.try_lock().unwrap();
        let mut contender = child(&alias, "probe");
        marker(&mut contender, "CONTENDED");
        assert!(contender.wait().unwrap().success());
    }

    fn publication(root: &Path) -> LocalPublication {
        let store = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
        let publication = LocalPublication::new(store.clone(), root.to_path_buf());
        assert!(Arc::ptr_eq(&publication.store, &store));
        publication
    }

    #[tokio::test]
    async fn aliases_and_overlapping_roots_share_subprocess_lock() {
        let temp = tempfile::tempdir().unwrap();
        let real = temp.path().join("real");
        #[cfg(unix)]
        let alias = temp.path().join("alias");
        #[cfg(not(unix))]
        let alias = real.clone();
        std::fs::create_dir(&real).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let broad = publication(temp.path());
        let narrow = publication(&alias);
        let mut holder = child(&alias, "primitive");
        marker(&mut holder, "LOCKED");
        let destinations = ["real/object".into()];
        let mut broad_waiting = Box::pin(broad.lock_parents(&destinations));
        assert!(futures::poll!(broad_waiting.as_mut()).is_pending());
        writeln!(holder.stdin.as_mut().unwrap(), "release").unwrap();
        assert!(holder.wait().unwrap().success());
        drop(
            tokio::time::timeout(std::time::Duration::from_secs(5), broad_waiting)
                .await
                .unwrap()
                .unwrap(),
        );
        let guards = broad
            .lock_parents(&[real.join("object"), alias.join("other")])
            .await
            .unwrap();
        assert_eq!(guards.len(), 1);
        let mut contender = child(&alias, "probe");
        marker(&mut contender, "CONTENDED");
        assert!(contender.wait().unwrap().success());
        let paths = ["object".into()];
        let mut waiting = Box::pin(narrow.lock_parents(&paths));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(guards);
        let guards = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(guards.len(), 1);
        drop(guards);
        assert!(real.join(LOCK_NAME).exists());
    }

    #[tokio::test]
    async fn opposite_inputs_deduplicate_and_acquire_in_physical_order() {
        let temp = tempfile::tempdir().unwrap();
        let mut parents = [temp.path().join("a"), temp.path().join("b")];
        for parent in &parents {
            std::fs::create_dir(parent).unwrap();
        }
        parents.sort_by_key(|parent| directory_identity(parent).unwrap());
        let held = open_lock(&parents[0]);
        held.try_lock().unwrap();
        let publication = publication(temp.path());
        let reverse = [parents[1].join("one"), parents[0].join("two")];
        let forward = [
            parents[0].join("three"),
            parents[1].join("four"),
            parents[0].join("five"),
        ];
        let mut first = Box::pin(publication.lock_parents(&reverse));
        let mut second = Box::pin(publication.lock_parents(&forward));
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(futures::poll!(second.as_mut()).is_pending());
        drop(held);
        let guards = tokio::time::timeout(std::time::Duration::from_secs(5), second)
            .await
            .expect("reverse input must not hold the later directory while awaiting the first")
            .unwrap();
        assert_eq!(guards.len(), 2);
        drop(guards);
        let guards = tokio::time::timeout(std::time::Duration::from_secs(5), first)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(guards.len(), 2);
    }

    #[tokio::test]
    async fn cancellation_releases_prior_guards_without_orphan_waiter() {
        let temp = tempfile::tempdir().unwrap();
        let mut parents = [temp.path().join("a"), temp.path().join("b")];
        for parent in &parents {
            std::fs::create_dir(parent).unwrap();
        }
        parents.sort_by_key(|parent| directory_identity(parent).unwrap());
        let mut holder = child(&parents[1], "hold");
        marker(&mut holder, "LOCKED");
        let publication = publication(temp.path());
        let paths = [parents[1].join("one"), parents[0].join("two")];
        let mut waiting = Box::pin(publication.lock_parents(&paths));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        assert!(matches!(
            open_lock(&parents[0]).try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        drop(waiting);
        open_lock(&parents[0]).try_lock().unwrap();
        writeln!(holder.stdin.as_mut().unwrap(), "release").unwrap();
        assert!(holder.wait().unwrap().success());
        let guards = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            publication.lock_parents(&paths),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(guards.len(), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_only_existing_lock_and_permission_errors() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("existing");
        std::fs::create_dir(&parent).unwrap();
        drop(open_lock(&parent));
        std::fs::set_permissions(
            parent.join(LOCK_NAME),
            std::fs::Permissions::from_mode(0o444),
        )
        .unwrap();
        let publication = publication(temp.path());
        let guards = publication
            .lock_parents(&["existing/object".into()])
            .await
            .unwrap();
        assert_eq!(guards.len(), 1);
        drop(guards);
        let denied = temp.path().join("denied");
        std::fs::create_dir(&denied).unwrap();
        std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o555)).unwrap();
        let result = publication.lock_parents(&["denied/object".into()]).await;
        std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            matches!(result, Err(StorageError::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied)
        );
    }
}
