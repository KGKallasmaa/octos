//! Session mutation regressions through the real gateway and persistence paths.
#![cfg(feature = "api")]
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use octos_bus::{ApiChannel, Channel, SessionHandle, SessionManager};
use octos_core::{Message, SessionKey};

const TOKEN: &str = "t0193-gateway-test-token";
const ID: &str = "web-legacy";

fn key() -> SessionKey {
    SessionKey::with_profile("_main", "api", ID)
}

struct Gateway {
    port: u16,
    task: tokio::task::JoinHandle<eyre::Result<()>>,
}
impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn gateway(dir: &Path) -> Gateway {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let channel = ApiChannel::new(
        port,
        Some(TOKEN.into()),
        Arc::new(AtomicBool::new(false)),
        Arc::new(tokio::sync::Mutex::new(SessionManager::open(dir).unwrap())),
        None,
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let task = tokio::spawn(async move { channel.start(tx).await });
    for _ in 0..300 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return Gateway { port, task };
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("gateway did not bind");
}
async fn delete(g: &Gateway) -> u16 {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .delete(format!("http://127.0.0.1:{}/sessions/{ID}", g.port))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}
async fn rename(g: &Gateway, title: &str) -> u16 {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .patch(format!("http://127.0.0.1:{}/sessions/{ID}/title", g.port))
        .bearer_auth(TOKEN)
        .header("content-type", "application/json")
        .body(serde_json::json!({ "title": title }).to_string())
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}
fn per_user(dir: &Path) -> PathBuf {
    let base = octos_bus::session::encode_path_component(key().base_key());
    dir.join("users")
        .join(base)
        .join("sessions")
        .join("default.jsonl")
}
fn lines(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .map(|s| s.lines().count())
        .unwrap_or(0)
}
async fn only_per_user_copy(dir: &Path) {
    let mut h = SessionHandle::open(dir, &key());
    h.add_message(Message::user("the only copy")).await.unwrap();
}
fn texts(messages: &[Message]) -> Vec<String> {
    messages.iter().map(|m| m.content.clone()).collect()
}

#[tokio::test]
async fn should_distinguish_truly_missing_sessions() {
    let d = tempfile::tempdir().unwrap();
    let g = gateway(d.path()).await;
    let (del, ren) = (delete(&g).await, rename(&g, "x").await);
    println!("a0 missing: delete={del} rename={ren}");
    assert_eq!((del, ren), (204, 404));
}

#[tokio::test]
async fn should_mutate_readable_sessions() {
    let d = tempfile::tempdir().unwrap();
    only_per_user_copy(d.path()).await;
    let g = gateway(d.path()).await;
    let ren = rename(&g, "ok").await;
    let del = delete(&g).await;
    println!(
        "a1 readable: rename={ren} delete={del} file_exists={}",
        per_user(d.path()).exists()
    );
    assert_eq!((ren, del), (204, 204));
    assert!(!per_user(d.path()).exists());
}

#[tokio::test]
#[cfg(unix)]
async fn should_report_permission_errors_and_allow_retry() {
    let d = tempfile::tempdir().unwrap();
    only_per_user_copy(d.path()).await;
    let f = per_user(d.path());
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&f).is_ok() {
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o644)).unwrap();
        eprintln!("privileged runner bypasses EACCES fixture");
        return;
    }
    let g = gateway(d.path()).await;
    let (del, ren) = (delete(&g).await, rename(&g, "x").await);
    let still = f.exists();
    std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o644)).unwrap();
    let fresh = SessionManager::open(d.path()).unwrap();
    let back = fresh.load(&key()).await.map(|s| texts(&s.messages));
    println!(
        "a2 eacces: delete={del} rename={ren} file_still_there={still} after_chmod_back={back:?}"
    );
    assert_eq!((del, ren), (500, 500));
    assert!(still);
    assert_eq!(rename(&g, "retry").await, 204);
    assert_eq!(delete(&g).await, 204);
}

#[tokio::test]
async fn should_report_directory_read_errors() {
    let d = tempfile::tempdir().unwrap();
    only_per_user_copy(d.path()).await;
    let f = per_user(d.path());
    std::fs::remove_file(&f).unwrap();
    std::fs::create_dir(&f).unwrap();
    let g = gateway(d.path()).await;
    let (del, ren) = (delete(&g).await, rename(&g, "x").await);
    println!(
        "a3 directory: delete={del} rename={ren} still_there={}",
        f.exists()
    );
    assert_eq!((del, ren), (500, 500));
    assert!(f.exists());
}

#[tokio::test]
async fn should_report_invalid_utf8() {
    let d = tempfile::tempdir().unwrap();
    only_per_user_copy(d.path()).await;
    let f = per_user(d.path());
    std::fs::write(&f, [0xff_u8, 0xfe, 0x00, 0x0a, 0xff, 0x0a]).unwrap();
    let g = gateway(d.path()).await;
    let (del, ren) = (delete(&g).await, rename(&g, "x").await);
    println!(
        "a4 corrupt: delete={del} rename={ren} still_there={}",
        f.exists()
    );
    assert_eq!((del, ren), (500, 500));
    assert!(f.exists());
}

/// The actor writes through SessionHandle; the gateway's SessionManager keeps its own LRU copy.
#[tokio::test]
async fn should_keep_actor_writes_between_renames() {
    let d = tempfile::tempdir().unwrap();
    let mut actor = SessionHandle::open(d.path(), &key());
    actor.add_message(Message::user("m1")).await.unwrap();
    let g = gateway(d.path()).await;
    assert_eq!(rename(&g, "first").await, 204);
    actor.add_message(Message::user("m2")).await.unwrap();
    actor.add_message(Message::user("m3")).await.unwrap();
    let before = lines(&per_user(d.path()));
    assert_eq!(rename(&g, "second").await, 204);
    let after = lines(&per_user(d.path()));
    let fresh = SessionManager::open(d.path())
        .unwrap()
        .load(&key())
        .await
        .unwrap();
    println!(
        "b1 per_user lines before={before} after={after}; cold manager={:?} title={:?}",
        texts(&fresh.messages),
        fresh.title
    );
    assert_eq!(
        texts(&fresh.messages),
        ["m1", "m2", "m3"],
        "history written since the first rename was lost"
    );
}

/// Same, while the gateway process has no first rename: only a cached earlier read would matter.
#[tokio::test]
async fn should_keep_actor_writes_before_first_rename() {
    let d = tempfile::tempdir().unwrap();
    let mut actor = SessionHandle::open(d.path(), &key());
    actor.add_message(Message::user("m1")).await.unwrap();
    let g = gateway(d.path()).await;
    actor.add_message(Message::user("m2")).await.unwrap();
    assert_eq!(rename(&g, "only").await, 204);
    let fresh = SessionManager::open(d.path())
        .unwrap()
        .load(&key())
        .await
        .unwrap();
    println!(
        "b2 cold manager={:?} title={:?}",
        texts(&fresh.messages),
        fresh.title
    );
    assert_eq!(texts(&fresh.messages), ["m1", "m2"]);
}

#[tokio::test]
async fn should_preserve_manual_title_when_actor_rewrites_after_next_turn() {
    let d = tempfile::tempdir().unwrap();
    let mut actor = SessionHandle::open(d.path(), &key());
    actor.add_message(Message::user("first")).await.unwrap();
    let g = gateway(d.path()).await;
    assert_eq!(rename(&g, "manual").await, 204);
    actor.add_message(Message::user("next turn")).await.unwrap();
    actor.sort_by_timestamp();
    actor.rewrite().await.unwrap();
    let persisted: serde_json::Value = serde_json::from_str(
        std::fs::read_to_string(per_user(d.path()))
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(persisted["title"], "manual");
    assert_eq!(persisted["title_manual"], true);
    assert_eq!(
        texts(
            &SessionManager::open(d.path())
                .unwrap()
                .load(&key())
                .await
                .unwrap()
                .messages
        ),
        ["first", "next turn"]
    );
}

#[tokio::test]
async fn should_preserve_sealed_segments_and_control_rows_on_rename() {
    for canonical in [false, true] {
        let d = tempfile::tempdir().unwrap();
        let mut manager = SessionManager::open(d.path()).unwrap();
        let active = if canonical {
            only_per_user_copy(d.path()).await;
            per_user(d.path())
        } else {
            manager
                .add_message(&key(), Message::user("sealed history"))
                .await
                .unwrap();
            manager.session_path(&key())
        };
        let sealed_dir = active.with_extension("segments");
        std::fs::create_dir(&sealed_dir).unwrap();
        let sealed = sealed_dir.join("000001.jsonl");
        let sealed_bytes = std::fs::read(&active).unwrap();
        std::fs::write(&sealed, &sealed_bytes).unwrap();
        let mut meta: serde_json::Value = serde_json::from_str(
            std::str::from_utf8(&sealed_bytes)
                .unwrap()
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        meta["sealed_segments"] = 1.into();
        meta["base_seq"] = 1.into();
        meta["future_metadata"] = "preserved".into();
        let rows = format!(
            "{}\n{}\n",
            serde_json::to_string(&Message::user("rolled back")).unwrap(),
            serde_json::json!({"kind":"rollback", "num_turns":1, "at":chrono::Utc::now()})
        );
        std::fs::write(&active, format!("{meta}\n{rows}")).unwrap();
        assert!(
            manager
                .update_title_existing(&key(), "manual".into())
                .await
                .unwrap()
        );
        let after = std::fs::read_to_string(&active).unwrap();
        assert_eq!(after.split_once('\n').unwrap().1, rows);
        assert_eq!(std::fs::read(&sealed).unwrap(), sealed_bytes);
        let after_meta: serde_json::Value =
            serde_json::from_str(after.lines().next().unwrap()).unwrap();
        meta["title"] = "manual".into();
        meta["title_manual"] = true.into();
        assert_eq!(after_meta, meta);
        assert_eq!(
            SessionManager::open(d.path())
                .unwrap()
                .load_full(&key())
                .await
                .unwrap()
                .messages
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn should_reject_malformed_metadata_and_rows_before_mutation_in_both_layouts() {
    for canonical in [false, true] {
        for fault in ["meta", "row", "future"] {
            let d = tempfile::tempdir().unwrap();
            let mut manager = SessionManager::open(d.path()).unwrap();
            let active = if canonical {
                only_per_user_copy(d.path()).await;
                per_user(d.path())
            } else {
                manager
                    .add_message(&key(), Message::user("history"))
                    .await
                    .unwrap();
                manager.session_path(&key())
            };
            let original = std::fs::read_to_string(&active).unwrap();
            let damaged = match fault {
                "meta" => "{broken}\n".to_owned(),
                "row" => format!("{original}{{broken row}}\n"),
                _ => original.replacen("\"schema_version\":2", "\"schema_version\":999", 1),
            };
            std::fs::write(&active, &damaged).unwrap();
            assert!(
                manager
                    .update_title_existing(&key(), "refused".into())
                    .await
                    .is_err()
            );
            assert!(manager.clear_existing(&key()).await.is_err());
            assert_eq!(std::fs::read_to_string(&active).unwrap(), damaged);
            std::fs::write(&active, &original).unwrap();
            assert!(
                manager
                    .update_title_existing(&key(), "retry".into())
                    .await
                    .unwrap()
            );
            assert!(manager.clear_existing(&key()).await.unwrap());
        }
    }
}

#[tokio::test]
async fn should_preserve_manual_title_when_stale_actor_rolls_a_segment() {
    let d = tempfile::tempdir().unwrap();
    let mut actor = SessionHandle::open(d.path(), &key());
    actor
        .add_message(Message::user("x".repeat(8 * 1024 * 1024 + 1024)))
        .await
        .unwrap();
    let mut manager = SessionManager::open(d.path()).unwrap();
    assert!(
        manager
            .update_title_existing(&key(), "manual before roll".into())
            .await
            .unwrap()
    );
    actor
        .add_message(Message::user("next segment"))
        .await
        .unwrap();
    assert!(
        per_user(d.path())
            .with_extension("segments")
            .join("000001.jsonl")
            .is_file()
    );
    let fresh = manager.load_full(&key()).await.unwrap();
    assert_eq!(fresh.messages.len(), 2);
    assert_eq!(fresh.title.as_deref(), Some("manual before roll"));
    assert!(fresh.title_manual);
}
