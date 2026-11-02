use super::*;

#[tokio::test]
async fn session_mutation_shared_gateway_failure_never_writes_locally() {
    for canonical in [false, true] {
        for deleting in [false, true] {
            let gateway =
                GatewayStub::start(StatusCode::INTERNAL_SERVER_ERROR, Some("owner-token")).await;
            let (dir, state) = legacy_fixture(MAIN_PROFILE_ID, false).await;
            let key = if canonical {
                SessionKey::new("api", "web-legacy")
            } else {
                SessionKey::with_profile(MAIN_PROFILE_ID, "api", "web-legacy")
            };
            state
                .sessions
                .as_ref()
                .unwrap()
                .lock()
                .await
                .add_message(&key, Message::user("preserve me"))
                .await
                .unwrap();
            register_gateway_at(
                &state,
                MAIN_PROFILE_ID,
                gateway.port,
                Some("owner-token"),
                Some(dir.path().join("data").to_str().unwrap().into()),
            )
            .await;
            let request = if canonical {
                key.clone()
            } else {
                SessionKey("web-legacy".into())
            };
            let response = if deleting {
                delete(&state, &request, Some(AuthIdentity::Admin)).await
            } else {
                rename(&state, &request, Some(AuthIdentity::Admin)).await
            };
            assert_eq!(
                response.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "canonical={canonical}, deleting={deleting}"
            );
            let fresh = octos_bus::SessionManager::open(&dir.path().join("data"))
                .unwrap()
                .load(&key)
                .await
                .unwrap();
            assert_eq!(fresh.messages.len(), 1);
            assert!(!fresh.title_manual);
            let calls = gateway.calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(
                calls[0].authorization.as_deref(),
                Some("Bearer owner-token")
            );
        }
    }
}

#[tokio::test]
async fn session_mutation_shared_gateway_success_does_not_duplicate_local_write() {
    for deleting in [false, true] {
        let gateway = GatewayStub::start(StatusCode::NO_CONTENT, None).await;
        let (dir, state) = legacy_fixture(MAIN_PROFILE_ID, true).await;
        register_gateway_at(
            &state,
            MAIN_PROFILE_ID,
            gateway.port,
            None,
            Some(dir.path().join("data").to_str().unwrap().into()),
        )
        .await;
        assert_eq!(
            legacy_mutation(&state, deleting, AuthIdentity::Admin)
                .await
                .status(),
            StatusCode::NO_CONTENT
        );
        // The stub reports success but deliberately leaves the file untouched:
        // any change here would prove serve is still writing the child's store.
        let fresh = octos_bus::SessionManager::open(&dir.path().join("data"))
            .unwrap()
            .load(&SessionKey::with_profile(
                MAIN_PROFILE_ID,
                "api",
                "web-legacy",
            ))
            .await
            .unwrap();
        assert!(!fresh.title_manual);
        assert_eq!(fresh.messages.len(), 1);
    }
}
