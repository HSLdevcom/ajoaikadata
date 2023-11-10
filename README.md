# Ajoaikadata 3.0

This repository contains the codebase and scripts and documentations related to the PoC of the new Ajoaikadata 3.0 service.


## What is Ajoaikadata?

Ajoaikadata is a service for processing messages coming from EKE devices of trains. The service will parse the binary content to understandable format, process messages coming from balises to locate the train and analyze arrival and departure events from data stream, for example.


## Technology

The PoC uses [Apache Pulsar](https://pulsar.apache.org) messaging platform and [Bytewax](https://bytewax.io) streaming platform. The code itself is written in Python, and services are containerized with Docker.


## Directory structure

Different components are placed under subdirectories.

For more information of each service and software component, check the README inside the subdirectory.


## Installation and running services

TODO
