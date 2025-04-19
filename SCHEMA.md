# Cassandra Schema Design for FB Messenger MVP

This document outlines the schema design for implementing a simplified Facebook Messenger backend using Apache Cassandra. The goal is to support real-time messaging with high performance and scalability.

## Design Principles

1. **Query-first modeling:** Tables are designed based on the application's access patterns.
2. **Denormalization:** Duplicate data across tables for optimized reads.
3. **Efficient pagination:** Use clustering columns to support message ordering and scrollback.
4. **High write throughput:** Optimized for fast inserts across distributed nodes.

## Schema

### 1. Users

Stores registered user profiles.

```cql
CREATE TABLE users (
    user_id UUID PRIMARY KEY,
    username TEXT,
    created_at TIMESTAMP
);
```

### 2. Messages By Conversation

Stores messages under a conversation, sorted by time (newest first).

```cql
CREATE TABLE messages_by_conversation (
    conversation_id UUID,
    created_at TIMESTAMP,
    message_id UUID,
    sender_id UUID,
    message_text TEXT,
    PRIMARY KEY (conversation_id, created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
```

### 3. Conversations By User

Used to fetch conversations for a user, sorted by last activity.

```cql
CREATE TABLE user_conversations (
    user_id UUID,
    last_message_timestamp TIMESTAMP,
    conversation_id UUID,
    participant_ids SET<UUID>,
    PRIMARY KEY (user_id, last_message_timestamp, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_timestamp DESC, conversation_id ASC);
```

### 4. Conversation Metadata (Optional)

Holds metadata like when the conversation was started.

```cql
CREATE TABLE conversations (
    conversation_id UUID PRIMARY KEY,
    participant_ids SET<UUID>,
    created_at TIMESTAMP
);
```

## Query Patterns Supported

### ✅ Send a Message
- Insert into `messages_by_conversation`
- Update both users' `user_conversations` rows

### ✅ Get All Messages in a Conversation
- Query `messages_by_conversation` by `conversation_id`
- Use timestamp + LIMIT for pagination

### ✅ Get Messages Before a Timestamp
- Same as above with `created_at < X`

### ✅ Get User Conversations
- Query `user_conversations` by `user_id`
- Use `last_message_timestamp` for ordering and pagination

## Trade-offs & Considerations

- **Multiple Writes:** One message write affects at least 2–3 tables.
- **Storage Duplication:** Data is repeated but improves read performance.
- **Tombstones:** Handle deletions cautiously to avoid tombstone-related latency.
- **Consistency:** Rely on eventual consistency due to Cassandra’s AP model.


