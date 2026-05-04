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
StateRequest { topic_name, action: Get, source_id }
StateRequest { topic_name, action: GetChildren, parent_source_id }
StateRequest { topic_name, action: ScanCurrent }
```

Typical write flow:

```text
upsert("customers", "cust-1", "{\"name\":\"Ada\"}", "org-1")
upsert("customers", "cust-2", "{\"name\":\"Linus\"}", "org-1")
upsert("customers", "cust-1", "{\"name\":\"Ada Lovelace\"}", "org-1")
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
