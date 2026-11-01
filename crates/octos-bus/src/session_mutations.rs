//! Fallible mutations of existing transcripts. The tolerant replay loader is
//! deliberately unsuitable for deciding whether a destructive target exists.
use super::*;
use std::io::{Read, Write};

struct MutationFile {
    path: PathBuf,
    content: String,
    meta: serde_json::Map<String, serde_json::Value>,
    rows_start: usize,
}

fn read_mutation_file(path: PathBuf) -> Result<Option<MutationFile>> {
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
    let mut content = String::new();
    file.read_to_string(&mut content)
        .wrap_err("read session for mutation")?;
    let rows_start = content.find('\n').map_or(content.len(), |n| n + 1);
    let first = content[..rows_start].trim_end();
    let parsed: SessionMeta = serde_json::from_str(first).wrap_err("parse session metadata")?;
    if parsed.schema_version > CURRENT_SESSION_SCHEMA {
        return Err(eyre::eyre!("session schema is newer than this build"));
    }
    // A tolerant read would silently drop malformed rows. Mutations must
    // neither erase them nor claim an unreadable target does not exist.
    for line in content[rows_start..].lines() {
        if !line.trim().is_empty() && parse_timeline_row(line).is_none() {
            return Err(eyre::eyre!("invalid session transcript row"));
        }
    }
    let meta = serde_json::from_str(first)?;
    Ok(Some(MutationFile {
        path,
        content,
        meta,
        rows_start,
    }))
}

fn mutation_files(paths: [PathBuf; 2]) -> Result<Vec<MutationFile>> {
    paths
        .into_iter()
        .filter_map(|path| read_mutation_file(path).transpose())
        .collect()
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
            file.write_all(&self.content.as_bytes()[self.rows_start..])?;
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
    /// I/O, malformed data and unsupported schema failures remain errors.
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
        let key_copy = key.clone();
        let guard = persist_lock_for(key).lock_owned().await;
        let result = tokio::task::spawn_blocking(move || -> Result<bool> {
            // Keep the lock inside the blocking job so cancellation cannot
            // release it while disk writes are still running.
            let _guard = guard;
            let flat = paths[0].clone();
            // Validate both layouts before touching either one. Read under the
            // same lock used by SessionHandle writers, never from the LRU.
            let files = mutation_files(paths)?;
            let found = !files.is_empty();
            for file in files {
                file.set_title(&title)?;
            }
            if !found && create {
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
        let lock = persist_lock_for(key);
        let _guard = lock.lock().await;
        let paths = self.mutation_paths(key);
        let exists = tokio::task::spawn_blocking(move || {
            mutation_files(paths).map(|files| !files.is_empty())
        })
        .await
        .wrap_err("join session deletion preflight")??;
        self.clear(key).await?;
        Ok(exists)
    }

    /// Keep the general API's ability to set a title before the first message;
    /// HTTP/OUP mutations use `update_title_existing` and never create a row.
    pub async fn update_title(&mut self, key: &SessionKey, title: String) -> Result<()> {
        self.set_title_on_disk(key, title, true).await.map(|_| ())
    }
}
