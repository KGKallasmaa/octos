use super::*;

fn app_state(sessions: Arc<Mutex<SessionManager>>) -> (ApiState, mpsc::Receiver<InboundMessage>) {
    let (inbound_tx, inbound_rx) = mpsc::channel(16);
    (
        ApiState {
            inbound_tx,
            pending: Default::default(),
            watchers: Default::default(),
            auth_token: None,
            profile_id: Some(TEST_PROFILE_ID.to_owned()),
            sessions,
            task_query: None,
            task_cancel: None,
            task_relaunch: None,
            on_session_deleted: None,
            metrics_renderer: None,
            event_seq: Default::default(),
        },
        inbound_rx,
    )
}

async fn chat_body(state: &ApiState, chat: &str, topic: Option<&str>, thread: &str) -> Body {
    let app = Router::new()
        .route("/chat", post(handle_chat))
        .with_state(state.clone());
    let value = serde_json::json!({"message":"waiting", "session_id":chat, "topic":topic, "thread_id":thread});
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/chat")
                .header("content-type", "application/json")
                .body(Body::from(value.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    response.into_body()
}

async fn delete(state: &ApiState, id: &str) {
    let response = handle_delete_session(
        State(state.clone()),
        HeaderMap::new(),
        axum::extract::Path(id.to_owned()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn delete_closes_an_unfinished_chat_without_a_model_completion() {
    let dir = tempfile::tempdir().unwrap();
    let (state, mut inbound) = app_state(test_sessions_in(dir.path()));
    let body = chat_body(&state, "active", None, "old-turn").await;
    inbound.recv().await.unwrap();
    delete(&state, &format!("{TEST_PROFILE_ID}:api:active")).await;
    tokio::time::timeout(
        std::time::Duration::from_millis(250),
        axum::body::to_bytes(body, 10240),
    )
    .await
    .expect("deleted request must close without waiting for the model")
    .unwrap();
}

#[tokio::test]
async fn topic_requests_have_independent_streams_and_delete_scope() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _inbound) = app_state(test_sessions_in(dir.path()));
    let a = chat_body(&state, "shared", Some("a"), "ta").await;
    let b = chat_body(&state, "shared", Some("b"), "tb").await;
    let other = chat_body(&state, "other", Some("a"), "to").await;
    let a = tokio::spawn(axum::body::to_bytes(a, 10240));
    let b = tokio::spawn(axum::body::to_bytes(b, 10240));
    let other = tokio::spawn(axum::body::to_bytes(other, 10240));
    delete(&state, &format!("{TEST_PROFILE_ID}:api:shared#a")).await;
    tokio::time::timeout(std::time::Duration::from_millis(250), a)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!b.is_finished());
    assert!(!other.is_finished());
    delete(&state, &format!("{TEST_PROFILE_ID}:api:shared")).await;
    tokio::time::timeout(std::time::Duration::from_millis(250), b)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!other.is_finished());
    delete(&state, "other").await;
    tokio::time::timeout(std::time::Duration::from_millis(250), other)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

fn output(chat: &str, thread: &str, completion: bool) -> OutboundMessage {
    OutboundMessage {
        channel: "api".into(),
        chat_id: chat.into(),
        content: if completion {
            String::new()
        } else {
            "late reply".into()
        },
        reply_to: None,
        media: vec![],
        metadata: if completion {
            serde_json::json!({"thread_id":thread,"_completion":true})
        } else {
            serde_json::json!({"thread_id":thread,"streaming":true})
        },
    }
}

fn channel_for(state: &ApiState) -> ApiChannel {
    let mut channel = ApiChannel::new(
        0,
        None,
        Arc::new(AtomicBool::new(false)),
        state.sessions.clone(),
        state.profile_id.clone(),
    );
    channel.pending = state.pending.clone();
    channel.event_seq = state.event_seq.clone();
    channel
}

async fn finish(body: Body) -> String {
    let bytes = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        axum::body::to_bytes(body, 10240),
    )
    .await
    .expect("stream must end promptly")
    .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn deleted_turn_output_cannot_enter_or_close_a_new_thread() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _inbound) = app_state(test_sessions_in(dir.path()));
    let channel = channel_for(&state);
    let old = chat_body(&state, "same", Some("topic"), "old").await;
    delete(&state, "dspfac:api:same#topic").await;
    let old_events = finish(old).await;
    assert!(
        !old_events.contains("\"type\":\"done\""),
        "delete cannot fabricate completion"
    );
    let fresh = chat_body(&state, "same", Some("topic"), "fresh").await;
    channel.send(&output("same", "old", false)).await.unwrap();
    channel.send(&output("same", "old", true)).await.unwrap();
    channel
        .edit_message_bound("same", "ignored", "late edit", Some("old"))
        .await
        .unwrap();
    channel
        .send_raw_sse_bound("same", r#"{"type":"thinking"}"#, Some("old"))
        .await
        .unwrap();
    assert_eq!(state.pending.lock().await.entries.len(), 1);
    channel.send(&output("same", "fresh", true)).await.unwrap();
    let events = finish(fresh).await;
    assert!(events.contains("\"type\":\"done\""));
    assert!(events.contains("\"topic\":\"topic\""));
    assert!(!events.contains("late"));
    assert!(!events.contains("\"thread_id\":\"old\""));
    assert!(state.pending.lock().await.entries.is_empty());
}

#[tokio::test]
async fn completion_and_repeated_delete_are_idempotent_for_sibling_threads() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _inbound) = app_state(test_sessions_in(dir.path()));
    let channel = channel_for(&state);
    let a = chat_body(&state, "same", Some("topic"), "a").await;
    let b = chat_body(&state, "same", Some("other-topic"), "b").await;
    channel.send(&output("same", "a", true)).await.unwrap();
    assert!(finish(a).await.contains("\"type\":\"done\""));
    assert_eq!(state.pending.lock().await.entries.len(), 1);
    let completion = output("same", "b", true);
    let ((), sent) = tokio::join!(
        delete(&state, "dspfac:api:same#other-topic"),
        channel.send(&completion)
    );
    sent.unwrap();
    finish(b).await;
    delete(&state, "dspfac:api:same#topic").await;
    assert!(state.pending.lock().await.entries.is_empty());
}

#[tokio::test]
async fn delete_callbacks_keep_exact_profile_topic_and_all_thread_owners() {
    let dir = tempfile::tempdir().unwrap();
    let (mut state, _inbound) = app_state(test_sessions_in(dir.path()));
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let capture = calls.clone();
    state.on_session_deleted = Some(Arc::new(move |key| {
        capture.lock().unwrap().push(key.to_owned())
    }));
    let a = chat_body(&state, "shared", Some("a"), "a1").await;
    let a2 = handle_chat(State(state.clone()), HeaderMap::new(), Json(serde_json::from_value(serde_json::json!({"message":"queued","session_id":"shared","topic":"a","thread_id":"a2"})).unwrap())).await;
    let ack: serde_json::Value = serde_json::from_str(&finish(a2.into_body()).await).unwrap();
    assert_eq!(ack["status"], "queued");
    let b = chat_body(&state, "shared", Some("b"), "b1").await;
    delete(&state, "foreign:api:shared#a").await;
    assert_eq!(state.pending.lock().await.entries.len(), 2);
    assert!(calls.lock().unwrap().is_empty());
    delete(&state, "dspfac:api:shared#a").await;
    finish(a).await;
    assert_eq!(*calls.lock().unwrap(), ["dspfac:api:shared#a"]);
    assert_eq!(state.pending.lock().await.entries.len(), 1);
    delete(&state, "shared").await;
    finish(b).await;
    assert_eq!(
        *calls.lock().unwrap(),
        ["dspfac:api:shared#a", "shared", "dspfac:api:shared#b"]
    );
    assert!(state.pending.lock().await.entries.is_empty());
}

#[tokio::test]
async fn delete_failure_keeps_pending_and_the_error_retryable() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _inbound) = app_state(test_sessions_in(dir.path()));
    let body = chat_body(&state, "active", None, "a").await;
    // A transcript directory instead of a file fails even when run as root.
    let file = state
        .sessions
        .lock()
        .await
        .data_dir()
        .join("users/dspfac%3Aapi%3Aactive/sessions/default.jsonl");
    std::fs::create_dir_all(&file).unwrap();
    let response = handle_delete_session(
        State(state.clone()),
        HeaderMap::new(),
        axum::extract::Path("dspfac:api:active".into()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(state.pending.lock().await.entries.len(), 1);
    std::fs::remove_dir(&file).unwrap();
    delete(&state, "dspfac:api:active").await;
    finish(body).await;
    assert!(state.pending.lock().await.entries.is_empty());
}

#[tokio::test]
async fn stale_attachment_and_failed_admission_do_not_leak_pending() {
    let dir = tempfile::tempdir().unwrap();
    let (state, inbound) = app_state(test_sessions_in(dir.path()));
    let old = chat_body(&state, "chat", Some("a"), "first").await;
    drop(old);
    let fresh = chat_body(&state, "chat", Some("a"), "second").await;
    let pending = state.pending.lock().await;
    assert_eq!(pending.entries.len(), 1);
    assert!(
        pending
            .get(&TurnContext::new(
                "chat",
                Some("a".into()),
                ThreadId::new("first")
            ))
            .is_none()
    );
    drop(pending);
    let conflict = handle_chat(State(state.clone()), HeaderMap::new(), Json(serde_json::from_value(serde_json::json!({"message":"conflict","session_id":"chat","topic":"b","thread_id":"second"})).unwrap())).await;
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    drop(inbound);
    let failed = handle_chat(State(state.clone()), HeaderMap::new(), Json(serde_json::from_value(serde_json::json!({"message":"closed","session_id":"failed","thread_id":"never-admitted"})).unwrap())).await;
    assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(state.pending.lock().await.entries.len(), 1);
    delete(&state, "chat").await;
    finish(fresh).await;
    assert!(state.pending.lock().await.entries.is_empty());
}
