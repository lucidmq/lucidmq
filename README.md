<div align="center">
<p align="center">
    
![LucidMQ](https://user-images.githubusercontent.com/25624274/218341069-514ac1ec-0a06-4260-a229-c047dd531ac2.png)

**Simple-Ops Event Streaming. Build your real time applications without the headache of ops overhead.**

<a href="https://lucidmq.com/docs/">Documentation</a> •
<a href="https://lucidmq.com">Blog</a> 
    
![CI](https://github.com/lucidmq/lucidmq/actions/workflows/lucidmq.yml/badge.svg)
![MIT License](https://img.shields.io/badge/License-MIT-success)

</p>
</div>

> :warning: **This project is in Alpha Stage**: Expect breaking changes

---

## What is LucidMQ

LucidMQ is a lightweight messaging and state-store system built around Nolan, the storage engine in this repo. It supports both stream-style reads and compacted "latest value by key" reads.

The compacted API is centered around:

- `upsert(topic, source_id, payload, parent_source_id=None)`
- `delete(topic, source_id, parent_source_id=None)`
- `get(topic, source_id)`
- `get_children(topic, parent_source_id)`
- `scan_current(topic)`

More detailed behavior and client examples live in the subdirectory READMEs:

- [LucidMQ broker/server docs](lucidmq/README.md)
- [Python client usage](lucidmq-py/README.md)

### Repo Structure

The repository is a monorepo with the core LucidMQ pieces.

    ├── nolan          # Storage engine and compaction logic
    ├── lucidmq        # LucidMQ broker and server
    ├── lucidmq-py     # Python client library and integration tests

---

## Getting Started

Getting started is simple: run a LucidMQ server instance and use a client to write records or query current state.

### How to Run LucidMQ

#### Locally via Rust and Cargo

#### Requirements:
1. Rust and Cargo Installed

See more details here
- https://www.rust-lang.org/tools/install
- https://doc.rust-lang.org/book/ch01-01-installation.html

- See this info for more installation instructions https://capnproto.org/install.html

See the [README in the LucidMQ directory](lucidmq/README.md) for starting up the LucidMQ server.

For client-side usage examples, see the [Python README](lucidmq-py/README.md).


### Docker

1. Build the base Docker image
```
docker build -f images/RustBase.Dockerfile -t registry.nocaply.com/rust-base:latest .
```

2. Build the docker images locally:

```
docker build -f images/Lucidmq.Dockerfile -t lucidmq:latest .
```

2. Run the Docker Container
```
docker run -it -p 6969:6969 lucidmq:latest
```

#### Running the integration tests

LucidMQ's integration tests run on the pipeline via every push to main. During local development it may be useful to verify that things are working as intended. Luckily the test infrastructure in bundled in docker compose. This allows for the intrgration test architecture to be is extreamly portable and easy to use. There are integration test suites for each client library.

Pre-requisites:
- Download and install docker
- docker-compose also downloaded and installed

Run the python integration tests using the following command:
```
docker-compose -f docker-compose-python-integration.yml up --build --exit-code-from test-runner
```

---

## License

MIT
