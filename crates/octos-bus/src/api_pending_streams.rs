#[derive(Default)]
struct PendingStreams {
    entries: HashMap<(String, Option<String>), PendingStream>,
}

struct PendingStream {
    context: TurnContext,
    threads: std::collections::HashSet<String>,
    sender: SseSender,
}

impl PendingStream {
    fn context_for(&self, ctx: &TurnContext) -> TurnContext {
        TurnContext::new(
            ctx.session_id.clone(),
            self.context.topic.clone(),
            ctx.thread_id.clone(),
        )
    }
}

impl PendingStreams {
    fn key(ctx: &TurnContext) -> (String, Option<String>) {
        (ctx.session_id.clone(), ctx.topic.clone())
    }

    fn get(&self, ctx: &TurnContext) -> Option<&PendingStream> {
        self.entries.values().find(|stream| {
            stream.context.session_id == ctx.session_id
                && stream.threads.contains(ctx.thread_id_str())
                // Bound edit operations carry a thread but no topic. Recover
                // that topic only from the request's own ingress context.
                && (ctx.topic.is_none() || ctx.topic == stream.context.topic)
        })
    }

    fn insert(&mut self, ctx: TurnContext, sender: SseSender) {
        let stream = self
            .entries
            .entry(Self::key(&ctx))
            .or_insert_with(|| PendingStream {
                context: ctx.clone(),
                threads: Default::default(),
                sender,
            });
        stream.threads.insert(ctx.thread_id_str().to_owned());
    }

    fn remove(&mut self, ctx: &TurnContext) {
        if let Some(stream) = self.get(ctx) {
            let key = Self::key(&stream.context);
            self.entries.remove(&key);
        }
    }

    fn has_chat(&self, chat: &str) -> bool {
        self.entries.keys().any(|(id, _)| id == chat)
    }

    fn active(&self, chat: &str, topic: Option<&str>) -> bool {
        self.entries.keys().any(|(id, scope)| {
            id == chat && topic.is_none_or(|topic| scope.as_deref() == Some(topic))
        })
    }

    fn session_keys(&self, profile_id: Option<&str>) -> Vec<SessionKey> {
        self.entries
            .values()
            .map(|entry| {
                current_profile_api_session_key_with_topic(
                    profile_id,
                    &entry.context.session_id,
                    entry.context.topic.as_deref(),
                )
            })
            .collect()
    }

    fn remove_sessions(&mut self, profile_id: Option<&str>, deleted: &[SessionKey]) {
        self.entries.retain(|_, entry| {
            let owner = current_profile_api_session_key_with_topic(
                profile_id,
                &entry.context.session_id,
                entry.context.topic.as_deref(),
            );
            !deleted
                .iter()
                .any(|key| session_delete_matches(key, &owner))
        });
    }
}

fn session_delete_matches(deleted: &SessionKey, owner: &SessionKey) -> bool {
    deleted.base_key() == owner.base_key()
        && (deleted.topic().is_none() || deleted.topic() == owner.topic())
}
