# Distributed Computing final project 

## Basic File sharing
### Proto File
    -Describes the data structures and services in a structured format that can be automatically parsed and used to generate code.
    
## How to run (Containterized version)

    1. docker run -it --net chord-network --name node1 chord-node python chord_server.py --ip node1 --port 50051  
    2. docker run -it --net chord-network --name node2 chord-node python chord_server.py --ip node2 --port 50051 --bootstrap_ip node1 --bootstrap_port 50051

## How to run (localhost)
    1. python chord_server.py --ip localhost --port 5001 
    2. python chord_server.py --ip localhost --port 5002 --bootstrap_ip localhost --bootstrap_port 5001
        
### Team
    1. Suryakangeyan
    2. Sahil salim
    3. Aaksah 

