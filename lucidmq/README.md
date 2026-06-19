# LucidMQ

This subdirectory contains the LucidMQ broker/server implementation. It persists keyed records into Nolan and exposes both stream-style consume APIs and compacted state-store APIs.

## Running the Server

```bash
cargo run
```

Run with logging enabled:

```bash
RUST_LOG=info cargo run
```

## Record Model

LucidMQ stores canonical `StoredRecord` values with these fields:

- `source_id`: the primary key for a record
- `parent_source_id`: an optional grouping key
- `payload`: the current value for the record
- `op`: internal record metadata, usually `Upsert` on client-visible records
- `timestamp`: producer-side timestamp metadata

Tombstones are an internal storage detail. Clients append records, and the server/storage layer decides when tombstones are needed and when they can be removed by compaction.

## API Overview

### Writes

- `upsert(source_id, parent_source_id, payload)` appends a record and updates the latest visible value for a key

### State Reads

- `get(source_id)` returns the latest visible record for a key
- `get_children(parent_source_id)` returns the latest visible children for a parent
- `scan_current()` returns the current visible snapshot for the whole topic

### Stream Reads

- `consume(consumer_group, timeout)` returns records sequentially for a consumer group
- stream consumption still exists, but for compacted/stateful workloads the state APIs are the preferred read path

## Request Types

The broker accepts three main request families:

- `ProduceRequest` for writes
- `ConsumeRequest` for stream reads
- `StateRequest` for state-store reads

Wire requests and responses are JSON payloads prefixed by a 2-byte little-endian frame length. Top-level messages use a `type` field, for example `{"type":"StateRequest", ...}`.

`StateRequest` supports these actions:

- `Get`
- `GetChildren`
- `ScanCurrent`

`StateResponse` returns:

- `record` for `Get`
- `records` for `GetChildren` and `ScanCurrent`

Produce responses still return offsets, but they should be treated as internal/remappable implementation details rather than stable external identifiers.

## Example Shapes

Conceptually, the state requests look like:

```text
{"type":"StateRequest","topic_name":"customers","action":"Get","source_id":"cust-1"}
{"type":"StateRequest","topic_name":"customers","action":"GetChildren","parent_source_id":"org-1"}
{"type":"StateRequest","topic_name":"customers","action":"ScanCurrent"}
```

Typical write flow:

```json
{"type":"ProduceRequest","topic_name":"customers","messages":[{"timestamp":1710000000000,"source_id":"cust-1","parent_source_id":"org-1","payload":{"name":"Ada"}}]}
{"type":"ProduceRequest","topic_name":"customers","messages":[{"timestamp":1710000000001,"source_id":"cust-2","parent_source_id":"org-1","payload":{"name":"Linus"}}]}
{"type":"ProduceRequest","topic_name":"customers","messages":[{"timestamp":1710000000002,"source_id":"cust-1","parent_source_id":"org-1","payload":{"name":"Ada Lovelace"}}]}
```

## Terminology

### Broker

The broker acts as the main logic behind the LucidMQ service. It is responsible for topics, producers, consumers, and persistence behavior.

### Server

The server accepts incoming TCP connections and forwards parsed requests to the broker.

### Topic

A topic maps Nolan storage to a named stream/state namespace.

### Producer

A producer submits append requests to a single topic. The broker materializes those as upserts in the current state view.

### Consumer

A consumer reads records from a single topic in order for a consumer group.

### Consumer Group

A consumer group tracks where sequential stream reads resume for a topic.
