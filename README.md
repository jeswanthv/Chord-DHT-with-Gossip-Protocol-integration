# Distributed Computing final project 

## Basic File sharing
### Proto File
    -Describes the data structures and services in a structured format that can be automatically parsed and used to generate code.
## How to run
        1. 
        ```
        python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. data_struct.proto
        ```  
        - to compile proto file into py code.
        2. Replace file paths in both server.py and client.py.
        3. Start server and run the client.

        
        
### Team
    1. Suryakangeyan
    2. Sahil salim
    3. Aaksah 
