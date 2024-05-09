#!/bin/bash

# Run the first node which may not have a bootstrap node
docker run -d --net chord-network --name node1 chord-node python chord_server.py --ip node1 --port 50051

# Run additional 99 nodes
for i in {2..100}; do
    prev_node=$((i - 1))
    docker run -d --net chord-network --name node$i chord-node python chord_server.py --ip node$i --port 50051 --bootstrap_ip node1 --bootstrap_port 50051
done

echo "All 100 nodes have been started in the Chord network."
