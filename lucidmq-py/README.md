# LucidMQ-py

This directory contains the Python client implementation of the LucidMQ protocol.

It supports both write operations and compacted state-store queries, and it also contains the Python integration test suite used to verify client/server behavior.

## Client API

### Write Helpers

- `Producer.upsert(topic_name, source_id, payload, parent_source_id=None)`
- `Producer.delete(topic_name, source_id, parent_source_id=None)`

### State Read Helpers

- `StateStore.get(topic_name, source_id)`
- `StateStore.get_children(topic_name, parent_source_id)`
- `StateStore.scan_current(topic_name)`

### Stream Read Helper

- `Consumer.consume(topic_name, consumer_group)`

Use the state-store helpers when you want the latest visible value for a key or the current contents of a topic. Use the consumer helper when you want sequential stream-style reads.

## Quickstart

```python
from lucidmq_client import TopicManager, Producer, StateStore

HOST = "127.0.0.1"
PORT = 6969

with TopicManager(HOST, PORT) as topics, Producer(HOST, PORT) as writer, StateStore(HOST, PORT) as store:
    topics.create_topic("customers")

    writer.upsert("customers", b"cust-1", b'{"name":"Ada"}', b"org-1")
    writer.upsert("customers", b"cust-2", b'{"name":"Linus"}', b"org-1")

    customer = store.get("customers", b"cust-1")
    children = store.get_children("customers", b"org-1")
    current = store.scan_current("customers")

    print(customer)
    print(children)
    print(current)

    writer.delete("customers", b"cust-2", b"org-1")
    print(store.scan_current("customers"))

    topics.delete_topic("customers")
```

## Response Shape

State reads return dictionaries parsed from MessagePack:

- `get(...)` returns a response with `record`
- `get_children(...)` returns a response with `records`
- `scan_current(...)` returns a response with `records`

Each record contains:

- `source_id`
- `parent_source_id`
- `payload`
- `op`
- `timestamp`

## How to Build

1. Create a virtual environment

```bash
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt
```

## How to Run Integration Tests

```bash
PYTHONPATH=src pytest
```

To see tests with captured output disabled:

```bash
PYTHONPATH=src pytest -s
```

To run a specific class:

```bash
PYTHONPATH=src pytest test_lucidmq.py::{classname}
```
