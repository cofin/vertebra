use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use object_store::local::LocalFileSystem;

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

impl LocalPublication {
    pub(super) fn new(store: Arc<LocalFileSystem>, root: PathBuf) -> Self {
        Self { store, root }
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
    use crate::store::StorageError;
    use object_store::local::LocalFileSystem;
    use std::fs::{File, OpenOptions};
    use std::io::{BufRead, BufReader, Write};
    use std::path::Path;
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;

    const LOCK_NAME: &str = ".vtb-cas-lock#0";

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
