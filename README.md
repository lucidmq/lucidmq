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

LucidMQ is a streaming platform that focuses on providing low configuration and low operation overhead along with speed. It enables the creation of stream or queue based applications by providing a rock solid foundation and simple API's. It is made up of multiple modules that are each documented in their own subdirectory.

### Repo Structure

The repository is a monorepo with everything LucidMQ related. In the future some of these librarys may be split into their own repository. `LucidMQ` and it's storage system `Nolan` are all written in Rust. `lucidmq-py` provides client libraries for Python and Go respectively. These clients also have their own integration tests suites to do regression testing and verify correctness.

    ├── nolan          # The base library containing code for the commitlog
    ├── lucidmq        # Lucidmq broker and server
    ├── lucidmq-py     # Python client library and integration tests
    ├── go-lucidmq     # Go client library and integration tests
    └── protocol       # Cap N' Proto definition protocol used by client-server comunication

---

## Getting Started

Getting started is easy. Just run a server instance of LucidMQ(either from soure or a docker container) and pick a client to interact with your server(CLI and Python Clients avaliable for now).

### How to Run LucidMQ

#### Locally via Rust and Cargo

#### Requirements:
1. Rust and Cargo Installed
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

See more details here
- https://www.rust-lang.org/tools/install
- https://doc.rust-lang.org/book/ch01-01-installation.html

2. Cap N' Proto Installed
```bash
brew install capnp
```
- See this info for more installation instructions https://capnproto.org/install.html

See the [README in LucidMQ Directory](/lucidmq/README.md) for starting up the LucidMQ Server.

For a client to interact with your LucidMQ server instance, utilize the LucidMQ-CLI. Learn more at the [README](/lucidmq-cli/README.md) in that directory.


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

Run the golang integration tests using the following command:
```
docker-compose -f docker-compose-go-integration.yml up --build --exit-code-from test-runner
```

---

## License

MIT
