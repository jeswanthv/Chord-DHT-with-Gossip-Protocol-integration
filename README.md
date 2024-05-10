# Distributed Computing Final Project: Distributed File Sharing System Using Chord

  

This project implements a basic file sharing system using the Chord Distributed Hash Table (DHT) protocol. It can be run in a Dockerized environment or on localhost for development and testing purposes.

  

## Overview

  

The system is designed to allow file uploads, downloads, and a gossip protocol communication between nodes in the Chord network. The implementation details are specified in a protobuf file which defines the required data structures and services.

  

## Prerequisites

  

- Docker

- Python 3

- gRPC and protobuf

  

## Setup

  

### Building the Docker Image

  

Before running the containers, build the Docker image using the provided Dockerfile:

  

```bash

docker build -t chord-node .

```

  

### Creating a Docker Network

  

Create a custom network to allow containers to communicate on the same network:

  

```bash

docker  network  create  chord-network

```

  

## Running the System

  

### Dockerized Version

  

To test the system in a Dockerized environment, use the following commands to run the nodes:

  

1. Start the first node (acts as the bootstrap node):

```bash

docker run -it --net chord-network --name node1 chord-node python chord_server.py --ip node1 --port 50051 -i

```

  

2. Add more nodes to the network:

```bash

docker run -it --net chord-network --name node2 chord-node python chord_server.py --ip node2 --port 50051 --bootstrap_ip node1 --bootstrap_port 50051 -i

docker run -it --net chord-network --name node3 chord-node python chord_server.py --ip node3 --port 50051 --bootstrap_ip node1 --bootstrap_port 50051 -i

```

  

### Localhost

  

For running the nodes on localhost:

options:
  -h, --help            show this help message and exit
  --ip IP               IP address of the node
  --port PORT           Port number of the node
  --m M                 Number of bits in the hash space
  --bootstrap_ip BOOTSTRAP_IP
                        IP address of the bootstrap node
  --bootstrap_port BOOTSTRAP_PORT
                        Port number of the bootstrap node
  -i, --interactive     Interactive mode

1. Start the first node:

```bash

python chord_server.py --ip localhost --port 5001

```

  

2. Connect additional nodes:

```bash

python chord_server.py --ip localhost --port 5002 --bootstrap_ip localhost --bootstrap_port 5001

```

  

## Running a Large-scale Simulation

  

To simulate a network with 100 nodes, use the `simulation.sh` script which automates the creation of Docker containers and joins them into the Chord ring:

  

```bash

./simulation.sh

```

  

## Client Interface

  

The client interface allows interaction with the network without being part of the Chord ring. It can download and upload files, and send gossip messages:

  

```bash

python  client.py

```

  

## Team Members

  

- Aakash

- Sahil

- Suryakangeyan

  

## Contributions

  

```
