# Ajoaikadata 3.0

This repository contains the codebase and scripts and documentations related to the PoC of the new Ajoaikadata 3.0 service.


## What is Ajoaikadata?

Ajoaikadata is a service for processing messages coming from EKE devices of trains. The service will parse the binary content to understandable format, process messages coming from balises to locate the train and analyze arrival and departure events from data stream, for example.

In the PoC, Ajoaikadata will be used to fetch data from Azure Storage and send it to Postgres. In the future, the service could be connected to the existing streaming pipeline, for example with Pulsar.

## Technology

The PoC uses [Apache Pulsar](https://pulsar.apache.org) messaging platform (optional), PostgreSQL database (with TimescaleDB addon) and [Bytewax](https://bytewax.io) streaming platform. The code itself is written in Python, and services are containerized with Docker.


The tested versions are:
- Bytewax 0.18.2
- Python 3.11
- Pulsar 3.1
- Postgres 15

## Directory structure

- [`db`](./db/): Initialization scripts for the database. In the local dev setup, this will be mounted to the init directory of the db image.
- [`docs`](./docs) Additional documentation files for PoC, such as architecture diagrams.

- [`src`](./src/): Bytewax code. In the local dev setup, this will be mounted as a volume for bytewax application code. All bytewax applications are placed on the root level of the directory.
- [`src/connectors`](./src/connectors): Custom made Bytewax connectors. They can be used to send / receive data from Pulsar, read data from Azure Storage and csv directory (in a way that the execution can be parallelized) and insert data to the Postgres database.
- [`src/operations`](./src/operations): Directory for Bytewax pipeline operations. Some of the operations can be used on multiple Bytewax dataflow apps, so operations are located here to be imported.


- [`src/ekeparser`](./src/ekeparser): Module to parse EKE's binary messages to human readable format.
- [`src/util`](./src/util): Other related code used in dataflows. The most notable module is balise_registry, which contains the manually selected mapping from balise id's to stations and tracks.

- [`tests`](./tests/): Tests for the data flow and operations.

There is more information written in Readme documents of modules.

## Installation and running services

In general, Bytewax applications are started with Bytewax.run command, like this:
```
python -m bytewax.run <dataflow.module>
```

To simplify setup and running, service can be installed and started with Docker Compose.

First, create a .env file to store secrets and other configuration.

Template:
```
# Dates
START_DATE=2024-01-10           <-- the first date that will be imported to Ajoaikadata
END_DATE=2024-01-14             <-- the last date that will be imported

# Vehicles
VEHICLE_LIST=53,54              <-- comma separated list to filter vehicles, use only for debug

# Azure storage connections
AZ_STORAGE_CONNECTION_STRING=   <-- connection string to the Azure Storage account
AZ_STORAGE_CONTAINER=           <-- container name where EKE data is stored

# Postgres Connections
POSTGRES_CONN_STR=postgresql://postgres:password@db:5432/postgres

POSTGRES_PASSWORD=password      
POSTGRES_USER=postgres
POSTGRES_DB=postgres


# Bytewax
BYTEWAX_WORKER_COUNT=4          <-- How many workers will be deployed into one container
BYTEWAX_BATCH_SIZE=5000         <-- How large batches ajoaikadata will read from the source at once
```



### Without Pulsar (single dataflow)

Application can be launched as a single dataflow, so that Pulsar is not needed. This is probably a better approach for batch processing, because the setup is lighter.

Installation:
```
docker compose build
```

Starting up the system:
```
docker compose up -d
```

Shutdown:
```
docker compose down
```


### With Pulsar (multiple microservices)

Pulsar setup launches several Bytewax applications. The whole data pipeline of Ajoaikadata is splitted into a few logical parts, like reading the data, parsing the data, and analyzing events from the message stream. Pulsar is used as a messaging platform between microservices. The installation is similar, but at this time, give a new compose file as an parameter.


Installation:
```
docker compose -f compose-with-pulsar.yml build
```

Starting up the system:
```
docker compose -f compose-with-pulsar.yml up -d
```

Shutdown:
```
docker compose -f compose-with-pulsar.yml down
```


### Without Docker (for a reference)

Dataflow can be run without containerization. This method is not tested and could have some problems with envs, paths and imports.


Installation:
```
pip install -r requirements.txt
```

Starting up the system:
```
python run -m bytewax.run src.ajoaikadata
```

## Running tests

Install dependencies and pytest -package:
```
pip install -r requirements.txt
pip install pytest
```

Run all tests:
```
pytest
```

Currently, there are a few tests created for dev purposes but the coverage is not yet good. Before going for production, more tests needs to be implemented.

## Monitoring

The default configuration of Docker Compose sets up monitoring resources. It's done with Prometheus and Grafana. Monitoring can be opened on the browser from `http://localhost:3000` when the service is up and running.

