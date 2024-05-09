"""
This module defines a set of constants used throughout the Distributed File Sharing System implementation.

The constants include configuration settings such as the number of successors each node should track, 
the stabilization interval for maintaining the finger table, and directory paths for storing files and handling downloads.

Constants:
    SUCCESSOR_COUNT (int): Number of successors each node in the Chord DHT should maintain. 
                           Helps in ensuring fault tolerance and reliable lookup.

    M (int): The number of bits in the node identifiers, which determines the size of the hash space,
             affecting the maximum number of nodes that can participate in the network.

    STABILIZATION_INTERVAL (int): Time interval (in seconds) for running stabilization procedures
                                  to update references between nodes. This is crucial for maintaining
                                  the integrity of the Chord ring structure.

    STORE_DIR (str): Path to the directory where files are stored by the DHT nodes. This directory
                     is used to store the actual files being managed by the distributed hash table.

    DOWNLOAD_DIR (str): Path to the directory where files are downloaded from the DHT. This is used
                        primarily for retrieving files stored within the DHT by other nodes.

Usage:
    These constants can be imported and used throughout the Chord DHT implementation to maintain
    consistency and facilitate configuration management. For example:

        from constants import SUCCESSOR_COUNT, M, STABILIZATION_INTERVAL

Note:
    Modify these constants carefully as changes can affect the overall behavior and performance
    of the distributed system.
"""

SUCCESSOR_COUNT = 3
M = 10
STABALIZATION_INTERVAL = 2
STORE_DIR = "store"
DOWNLOAD_DIR = "downloads"
