# Ajoaikadata 3.0

This repository contains the codebase and scripts and documentations related to the PoC of the new Ajoaikadata 3.0 service.


## What is Ajoaikadata?

Ajoaikadata is a service for processing messages coming from EKE devices of trains. The service will parse the binary content to understandable format, process messages coming from balises to locate the train and analyze arrival and departure events from data stream, for example.


## Technology

The PoC uses [Apache Pulsar](https://pulsar.apache.org) messaging platform and [Bytewax](https://bytewax.io) streaming platform. The code itself is written in Python, and services are containerized with Docker.


## Directory structure

Different components are placed under subdirectories.

Bytewax programs:
- [`reader`](./reader/): Program to read archived messages from csv file to Pulsar.
- [`contentparser`](./contentparser/): Program to convert raw messages to readable format.
- [`sink`](./sink/): Program to store messages processed by data pipeline.

Helper libraries:
- [`connectors`](./connectors/): Custom connectors for Bytewax to connect, e.g., Pulsar.
- [`ekeparser`](./ekeparser/): Python library to parse EKE's binary messages.

Others:
- [`docs`](./docs) Additional documentation files for PoC.


For more information of each service and software component, check the README inside the subdirectory.


## Installation and running services

TODO
