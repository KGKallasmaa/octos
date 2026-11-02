//! Fallible mutations of existing transcripts. The tolerant replay loader is
//! deliberately unsuitable for deciding whether a destructive target exists.
use super::*;
use std::io::{Read, Write};

struct MutationFile {
    path: PathBuf,
    content: Vec<u8>,
    meta: serde_json::Map<String, serde_json::Value>,
    rows_start: usize,
}

fn read_mutation_file(
    path: PathBuf,
    key: &SessionKey,
    has_canonical_copy: bool,
) -> Result<Option<MutationFile>> {
    let mut file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A missing active file with sealed history is an interrupted
            // seal, not an absent session. Leave recovery to the replay path.
            if segments_dir(&path).try_exists()? {
                return Err(eyre::eyre!(
                    "session active file missing beside sealed history"
                ));
            }
            return Ok(None);
        }
        Err(error) => return Err(error).wrap_err("open session for mutation"),
    };
    if !file.metadata()?.is_file() {
        return Err(eyre::eyre!("session mutation target is not a file"));
    }
    let mut content = Vec::new();
    file.read_to_end(&mut content)
        .wrap_err("read session for mutation")?;
    let rows_start = content
        .iter()
        .position(|b| *b == b'\n')
        .map_or(content.len(), |n| n + 1);
    let first = &content[..rows_start];
    let parsed: SessionMeta = match serde_json::from_slice(first) {
        Ok(meta) => meta,
        Err(_) if has_canonical_copy => {
            // A trusted per-user copy establishes the session's identity. An
            // obsolete flat file may contain garbage; leave its bytes untouched
            // on rename, and allow explicit deletion to remove both layouts.
            tracing::warn!(path = %path.display(), "skipping obsolete flat session metadata");
            return Ok(None);
        }
        Err(error) => return Err(error).wrap_err("parse session metadata"),
    };
    if parsed.session_key != key.0 {
        return Err(eyre::eyre!(
            "session metadata does not match mutation target"
        ));
    }
    if parsed.schema_version > CURRENT_SESSION_SCHEMA {
        return Err(eyre::eyre!("session schema is newer than this build"));
    }
    // Only metadata is interpreted. Tolerant replay can still use a transcript
    // with a torn tail or malformed middle row; metadata edits preserve ALL
    // body bytes, including invalid UTF-8, rather than parsing/dropping rows.
    let meta = serde_json::from_slice(first)?;
    Ok(Some(MutationFile {
        path,
        content,
        meta,
        rows_start,
    }))
}

fn mutation_files(paths: [PathBuf; 2], key: &SessionKey) -> Result<Vec<MutationFile>> {
    let [flat, canonical] = paths;
    // Only malformed metadata in an obsolete flat copy can be ignored, and
    // only with a trusted canonical copy. I/O failures and incompatible schema
    // or identity remain errors in either layout, before either is mutated.
    let canonical = read_mutation_file(canonical, key, false)?;
    let flat = read_mutation_file(flat, key, canonical.is_some())?;
    Ok(flat.into_iter().chain(canonical).collect())
}

impl MutationFile {
    fn set_title(mut self, title: &str) -> Result<()> {
        self.meta.insert("title".into(), title.into());
        self.meta.insert("title_manual".into(), true.into());
        let tmp = rewrite_tmp_path(&self.path);
        let result = (|| -> Result<()> {
            let mut file = std::fs::File::create(&tmp)?;
            serde_json::to_writer(&mut file, &self.meta)?;
            file.write_all(b"\n")?;
            // Preserve every message/control row byte for byte, and never
            // collapse, copy, or delete sealed segments while renaming.
            file.write_all(&self.content[self.rows_start..])?;
            file.sync_all()?;
            std::fs::rename(&tmp, &self.path)?;
            if let Some(parent) = self.path.parent() {
                fsync_dir(parent);
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(tmp);
        }
        result
    }
}

impl SessionManager {
    fn mutation_paths(&self, key: &SessionKey) -> [PathBuf; 2] {
        [
            self.session_path(key),
            self.data_dir()
                .join("users")
                .join(encode_path_component(key.base_key()))
                .join("sessions")
                .join(SessionHandle::topic_filename(key)),
        ]
    }

    /// Rename an existing session. Only true absence returns `Ok(false)`;
    /// I/O, untrusted metadata and unsupported schema failures remain errors.
    pub async fn update_title_existing(&mut self, key: &SessionKey, title: String) -> Result<bool> {
        self.set_title_on_disk(key, title, false).await
    }

    async fn set_title_on_disk(
        &mut self,
        key: &SessionKey,
        title: String,
        create: bool,
    ) -> Result<bool> {
        let paths = self.mutation_paths(key);
        let root = self.data_dir();
        let key_copy = key.clone();
        let guard = persist_lock_for(key).lock_owned().await;
        let result = tokio::task::spawn_blocking(move || -> Result<bool> {
            // Keep the lock inside the blocking job so cancellation cannot
            // release it while disk writes are still running.
            let _guard = guard;
            let lifecycle = lifecycle::Guard::acquire(&root, &key_copy)?;
            let flat = paths[0].clone();
            // Validate both layouts before touching either one. Read under the
            // same lock used by SessionHandle writers, never from the LRU.
            let files = mutation_files(paths, &key_copy)?;
            let found = !files.is_empty();
            for file in files {
                file.set_title(&title)?;
            }
            if !found && create {
                lifecycle.check_live(&lifecycle.epoch())?;
                let mut session = Session::new(key_copy);
                session.title = Some(title);
                session.title_manual = true;
                SessionHandle::rewrite_blocking(&flat, &session)?;
            }
            Ok(found)
        })
        .await
        .wrap_err("join session title mutation")?;
        // A cache may also predate independent actor writes. Reload on next
        // access, including after a partial two-layout metadata write failure.
        self.cache.pop(&key.0);
        result
    }

    /// Delete only after a fallible disk read, serialized with session writes.
    pub async fn clear_existing(&mut self, key: &SessionKey) -> Result<bool> {
        let guard = persist_lock_for(key).lock_owned().await;
        let paths = self.mutation_paths(key);
        let key_copy = key.clone();
        let root = self.data_dir();
        let result = tokio::task::spawn_blocking(move || -> Result<bool> {
            let _guard = guard;
            let mut lifecycle = lifecycle::Guard::acquire(&root, &key_copy)?;
            let exists = !mutation_files(paths.clone(), &key_copy)?.is_empty();
            // Publish revocation BEFORE removing files. An interrupted/failed
            // delete stays revoked and retryable, never admits late writers.
            lifecycle.delete()?;
            for path in paths {
                remove_segments(&path)?;
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e).wrap_err("delete session transcript"),
                }
                if let Some(parent) = path.parent().filter(|p| p.is_dir()) {
                    lifecycle::sync_dir(parent)?;
                }
            }
            Ok(exists)
        })
        .await
        .wrap_err("join session deletion")?;
        self.cache.pop(&key.0);
        result
    }

    /// Keep the general API's ability to set a title before the first message;
    /// HTTP/OUP mutations use `update_title_existing` and never create a row.
    pub async fn update_title(&mut self, key: &SessionKey, title: String) -> Result<()> {
        self.set_title_on_disk(key, title, true).await.map(|_| ())
    }
}
