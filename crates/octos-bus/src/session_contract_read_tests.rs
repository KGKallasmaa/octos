use super::*;

fn corrupt(bytes: &[u8], fault: &str) -> Vec<u8> {
    let text = std::str::from_utf8(bytes).unwrap();
    let (header, rows) = text.split_once('\n').unwrap();
    match fault {
        "meta" => format!("{{broken meta}}\n{rows}").into_bytes(),
        "schema" => {
            let mut meta: serde_json::Value = serde_json::from_str(header).unwrap();
            meta["schema_version"] = (CURRENT_SESSION_SCHEMA + 1).into();
            format!("{meta}\n{rows}").into_bytes()
        }
        "utf8" => [header.as_bytes(), b"\n\xff\n", rows.as_bytes()].concat(),
        "row" => format!("{header}\n{{\"role\":\"unknown\"}}\n{rows}").into_bytes(),
        "empty" => vec![],
        _ => panic!("unknown fault"),
    }
}

fn contract(parent: &SessionKey) -> ChildSessionContract {
    ChildSessionContract {
        task_id: "strict-read".into(),
        task_label: "preserve transcript".into(),
        parent_session_key: parent.to_string(),
        child_session_key: child_session_key(parent, "strict-read").to_string(),
        workflow_kind: None,
        current_phase: None,
        terminal_state: None,
        join_state: None,
        joined_at: None,
        failure_action: None,
        error: None,
        output_files: vec![],
    }
}

#[tokio::test]
async fn canonical_contract_creates_a_genuinely_absent_session() {
    let tmp = tempfile::tempdir().unwrap();
    let key = SessionKey::new("api", "strict-new");
    assert!(
        !upsert_child_contract_through_canonical_path(tmp.path(), &key, contract(&key))
            .await
            .unwrap()
    );
    let reopened = SessionHandle::try_open(tmp.path(), &key).unwrap();
    assert!(reopened.session.messages.is_empty());
    assert_eq!(reopened.session.child_contracts.len(), 1);
}

#[tokio::test]
async fn canonical_contract_rejects_corrupt_transcripts_and_retries() {
    for fault in ["meta", "schema", "utf8", "row", "empty"] {
        let tmp = tempfile::tempdir().unwrap();
        let key = SessionKey::new("api", &format!("strict-{fault}"));
        let mut handle = SessionHandle::open(tmp.path(), &key);
        handle
            .add_message(Message::user("keep history"))
            .await
            .unwrap();
        let path = handle.session_path();
        let original = std::fs::read(&path).unwrap();
        let damaged = corrupt(&original, fault);
        std::fs::write(&path, &damaged).unwrap();
        let result =
            upsert_child_contract_through_canonical_path(tmp.path(), &key, contract(&key)).await;
        assert!(result.is_err(), "{fault}: {result:?}");
        assert_eq!(std::fs::read(&path).unwrap(), damaged, "{fault}");
        std::fs::write(&path, original).unwrap();
        upsert_child_contract_through_canonical_path(tmp.path(), &key, contract(&key))
            .await
            .unwrap();
        let reopened = SessionHandle::open_full(tmp.path(), &key);
        assert_eq!(reopened.session.messages.len(), 1);
        assert_eq!(reopened.session.messages[0].content, "keep history");
        assert_eq!(reopened.session.child_contracts.len(), 1);
    }
}

#[tokio::test]
async fn fork_rejects_corrupt_parent_or_child_and_retries() {
    for fault in ["meta", "schema", "utf8", "row", "empty"] {
        for damage_parent in [true, false] {
            let tmp = tempfile::tempdir().unwrap();
            let parent = SessionKey::new("api", &format!("fork-{fault}-{damage_parent}"));
            let child = child_session_key(&parent, "strict-child");
            let mut ph = SessionHandle::open(tmp.path(), &parent);
            ph.add_message(Message::user("parent history"))
                .await
                .unwrap();
            let mut ch = SessionHandle::open(tmp.path(), &child);
            ch.add_message(Message::user("child history"))
                .await
                .unwrap();
            let path = if damage_parent {
                ph.session_path()
            } else {
                ch.session_path()
            };
            let original = std::fs::read(&path).unwrap();
            let damaged = corrupt(&original, fault);
            std::fs::write(&path, &damaged).unwrap();
            let parent_before = std::fs::read(ph.session_path()).unwrap();
            let child_before = std::fs::read(ch.session_path()).unwrap();
            let result =
                SessionHandle::fork_from_parent_if_missing(tmp.path(), &parent, &child, 10).await;
            assert!(
                result.is_err(),
                "{fault}, parent={damage_parent}: {result:?}"
            );
            assert_eq!(std::fs::read(ph.session_path()).unwrap(), parent_before);
            assert_eq!(std::fs::read(ch.session_path()).unwrap(), child_before);
            std::fs::write(path, original).unwrap();
            SessionHandle::fork_from_parent_if_missing(tmp.path(), &parent, &child, 10)
                .await
                .unwrap();
            let reopened = SessionHandle::open_full(tmp.path(), &child);
            assert_eq!(reopened.session.messages.len(), 1);
            assert_eq!(reopened.session.messages[0].content, "child history");
            assert_eq!(reopened.session.parent_key.as_ref(), Some(&parent));
        }
    }
}

#[tokio::test]
async fn strict_contract_load_preserves_legacy_and_sealed_history() {
    for fault in ["meta", "schema", "utf8", "row", "empty"] {
        for legacy in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let key = SessionKey::new("api", &format!("strict-layout-{fault}-{legacy}"));
            let mut handle = SessionHandle::open(tmp.path(), &key);
            handle
                .add_message(Message::user("old history"))
                .await
                .unwrap();
            let active = handle.session_path();
            let original = std::fs::read(&active).unwrap();
            let source = if legacy {
                let path = SessionManager::session_path_static(&tmp.path().join("sessions"), &key);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::rename(&active, &path).unwrap();
                path
            } else {
                let dir = segments_dir(&active);
                std::fs::create_dir_all(&dir).unwrap();
                let path = segment_path(&dir, 1);
                std::fs::write(&path, &original).unwrap();
                let text = std::str::from_utf8(&original).unwrap();
                let mut meta: serde_json::Value =
                    serde_json::from_str(text.lines().next().unwrap()).unwrap();
                meta["sealed_segments"] = 1.into();
                meta["base_seq"] = 1.into();
                std::fs::write(&active, format!("{meta}\n")).unwrap();
                path
            };
            let damaged = corrupt(&original, fault);
            std::fs::write(&source, &damaged).unwrap();
            let before_active = std::fs::read(&active);
            let result =
                upsert_child_contract_through_canonical_path(tmp.path(), &key, contract(&key))
                    .await;
            assert!(result.is_err(), "{fault}, legacy={legacy}: {result:?}");
            assert_eq!(std::fs::read(&source).unwrap(), damaged);
            if legacy {
                assert!(!active.exists());
            } else {
                assert_eq!(std::fs::read(&active).unwrap(), before_active.unwrap());
            }
            std::fs::write(&source, original).unwrap();
            upsert_child_contract_through_canonical_path(tmp.path(), &key, contract(&key))
                .await
                .unwrap();
            let reopened = SessionHandle::try_open(tmp.path(), &key).unwrap();
            assert_eq!(reopened.session.messages.len(), 1);
            assert_eq!(reopened.session.messages[0].content, "old history");
            assert_eq!(reopened.session.child_contracts.len(), 1);
        }
    }
}

#[tokio::test]
async fn strict_contract_load_recovers_interrupted_seal_and_replays_rollback() {
    let tmp = tempfile::tempdir().unwrap();
    let key = SessionKey::new("api", "strict-interrupted-seal");
    let mut handle = SessionHandle::open(tmp.path(), &key);
    handle
        .add_message(Message::user("keep older turn"))
        .await
        .unwrap();
    handle
        .add_message(Message::user("rolled back turn"))
        .await
        .unwrap();
    let active = handle.session_path();
    let dir = segments_dir(&active);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::rename(&active, segment_path(&dir, 1)).unwrap();
    upsert_child_contract_through_canonical_path(tmp.path(), &key, contract(&key))
        .await
        .unwrap();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&active)
        .unwrap();
    use std::io::Write;
    writeln!(file, "{}", rollback_marker_line(1).unwrap()).unwrap();
    upsert_child_contract_through_canonical_path(tmp.path(), &key, contract(&key))
        .await
        .unwrap();
    let reopened = SessionHandle::try_open(tmp.path(), &key).unwrap();
    assert_eq!(reopened.session.messages.len(), 1);
    assert_eq!(reopened.session.messages[0].content, "keep older turn");
    assert_eq!(reopened.session.child_contracts.len(), 1);
}
