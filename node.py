"""
This module defines the Node class for managing nodes in a Chord distributed hash table (DHT). 
Each node in the network maintains information about its successor and predecessor nodes 
and a finger table for efficient key lookups.

The Node class encapsulates functionalities necessary for joining the network, maintaining 
the network structure through stabilization processes, handling key-value storage, and 
facilitating file uploads and downloads within the DHT.

Dependencies:
    grpc: For RPC communication between nodes in the DHT.
    prettytable: To display tables in a human-readable format for debugging and monitoring.
    proto (chord_pb2, chord_pb2_grpc): Contains the protobuf definitions for the DHT.
    utils: Includes utility functions such as create_stub for gRPC stub creation, is_within_bounds
           to check numerical boundaries, sha1_hash for hashing keys, and generate_requests for 
           constructing file transfer requests.
    random, ast: Standard Python libraries used for random choices and safely evaluating strings 
                 into Python expressions respectively.
    constants: Configurations such as SUCCESSOR_COUNT are defined here.
"""


import grpc
from prettytable import PrettyTable, ALL

from proto import chord_pb2
from proto import chord_pb2_grpc
from utils import create_stub, is_within_bounds, sha1_hash, generate_requests, logger
import random
import ast
from constants import SUCCESSOR_COUNT
import os

os.environ['GRPC_VERBOSITY'] = 'NONE'


class Node:
    """
    Represents a node in a Chord distributed hash table (DHT).

    This class encapsulates the functionalities necessary for node operations within the Chord network,
    including joining and leaving the network, maintaining the network's stability through periodic operations,
    handling key-value storage, and facilitating file uploads and downloads.

    Attributes:
        ip (str): IP address of the node.
        port (int): Port number on which the node listens for incoming connections.
        node_id (int): Unique identifier for the node within the Chord ring.
        m (int): The number of bits in the hash keys used in the Chord protocol, determining the ring's size.
        finger_table (dict): A dictionary representing the finger table. Each entry maps an index to a node,
                            helping in efficient routing.
        predecessor (Node): The node's immediate predecessor in the Chord ring.
        successor (Node): The node's immediate successor in the Chord ring.
        successor_list ([Node]): A list of successors for reliability and fault tolerance.
        store (dict): A dictionary representing the key-value store located at this node.
        received_gossip_message_ids (set): A set of IDs of gossip messages received to prevent re-processing.

    Methods:
        __init__(self, node_id, ip, port, m): Initializes a new node with the given identifier, IP address,
                                            port, and hash key length.
        join_chord_ring(self, bootstrap_node): Joins the Chord ring using the given bootstrap node if provided.
                                            If no bootstrap node is given, initializes a new Chord ring.
        stabilize(self): Periodically verifies the node's immediate successor and updates necessary references.
        fix_fingers(self): Updates the finger table entries periodically to maintain efficient routing capability.
        update_other_nodes(self): Notifies other nodes to update their finger tables with this node's information.
        leave(self): Properly leaves the Chord network, transferring keys and updating other nodes' references.

    Usage:
        A node can be instantiated and then used to join a network:
            node = Node(node_id=123, ip='192.168.1.1', port=50051, m=160)
            node.join_chord_ring(bootstrap_node=None)  # Starts a new Chord ring if no bootstrap node is provided

        Once part of the network, the node will automatically handle its responsibilities through internal methods,
        but methods like leave() can be called to manually remove the node from the network.
    """

    def __init__(self, node_id: int, ip: str, port: int, m):
        self.ip = str(ip)
        self.port = int(port)
        self.node_id = int(node_id)
        self.m = m
        self.finger_table = {}
        self.predecessor = None
        self.successor = None
        self.finger_table = {i: self for i in range(m)}
        self.successor_list = [self for _ in range(SUCCESSOR_COUNT)]
        self.store = {}
        self.received_gossip_message_ids = set()
        self.received_gossip_messages = []

    def __str__(self):
        return f"{self.ip}:{self.port}"

    def __repr__(self):
        return f"{self.ip}:{self.port}"

    def join_chord_ring(self, bootstrap_node):
        """
        Joins this node to an existing Chord distributed hash table using a provided bootstrap node or starts a new ring if no bootstrap node is available.

        This method handles the full process of integrating the node into the Chord network, including setting up initial connections,
        determining its predecessor and successor, initializing its finger table, and informing other nodes of its arrival to ensure the network's
        integrity and efficiency.

        Args:
            bootstrap_node (Node): The bootstrap node already in the Chord network used to discover the network topology. If `None`, this node
                                will start a new Chord ring and initialize itself as its own predecessor and successor.

        Steps:
            - If a bootstrap node is provided, the method:
            - Establishes a connection with the bootstrap node to find its immediate predecessor and successor within the network.
            - Sets up the local node's predecessor and successor based on responses from the network.
            - Initializes the finger table for efficient future lookups.
            - Updates other nodes' finger tables to include this node for consistent network routing.
            - If no bootstrap node is provided, initializes the node as a standalone ring.
            - Handles all network communications and updates through robust error checking and retry mechanisms to manage any potential communication failures.
        """

        logger.info(f"[Node ID: {self.node_id}] Starting join ....")

        if not bootstrap_node:
            logger.info(
                f"[Node ID: {self.node_id}] No bootstrap server provided. Starting a new chord ring.")
            self.successor = self
            self.predecessor = self
            for i in range(self.m):
                self.finger_table[i] = self
        else:
            try:
                logger.info(
                    f"[Node ID: {self.node_id}] Joining an existing chord ring with bootstrap server {bootstrap_node}.")
                bootstrap_stub, bootstrap_channel = create_stub(
                    bootstrap_node.ip, bootstrap_node.port)

                with bootstrap_channel:

                    try:
                        find_predecessor_request = chord_pb2.FindPredecessorRequest(
                            id=self.node_id)
                        find_predecessor_response = bootstrap_stub.FindPredecessor(
                            find_predecessor_request, timeout=5)
                    except Exception as e:
                        logger.error(
                            f"[Node ID: {self.node_id}] Error connecting to the bootstrap node: {e}")
                        return

                    self.predecessor = Node(find_predecessor_response.id,
                                            find_predecessor_response.ip,
                                            find_predecessor_response.port, self.m)
                    logger.info(
                        f"[Node ID: {self.node_id}] Found predecessor node {self.predecessor}.")

                self_stub, self_channel = create_stub(self.ip, self.port)
                with self_channel:
                    set_predecessor_request = chord_pb2.NodeInfo(
                        id=self.predecessor.node_id, ip=self.predecessor.ip, port=self.predecessor.port
                    )
                    self_stub.SetPredecessor(
                        set_predecessor_request, timeout=5)

                predecessor_stub, predecessor_channel = create_stub(
                    self.predecessor.ip, self.predecessor.port)

                with predecessor_channel:
                    try:
                        get_successor_request = chord_pb2.Empty()
                        get_successor_response = predecessor_stub.GetSuccessor(
                            get_successor_request, timeout=5)
                        self.successor = Node(get_successor_response.id,
                                              get_successor_response.ip,
                                              get_successor_response.port, self.m)
                        logger.info(
                            f"[Node ID: {self.node_id}] Found successor node {self.successor}.")
                    except Exception as e:
                        logger.error(
                            f"[Node ID: {self.node_id}] Error connecting to the predecessor node: {e}")
                        return

                self_stub, self_channel = create_stub(self.ip, self.port)
                with self_channel:
                    set_successor_request = chord_pb2.NodeInfo(
                        id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port
                    )
                    self_stub.SetSuccessor(set_successor_request, timeout=5)

                self.initialize_finger_table(bootstrap_node)
                logger.info(
                    f"[Node ID: {self.node_id}] Finger table initialized successfully.")

                logger.info(
                    f"[Node ID: {self.node_id}] Starting to update nodes in the chord ring.")
                self.update_other_nodes()
                logger.info(
                    f"[Node ID: {self.node_id}] Successfully updated other nodes about this join.")

                logger.info(
                    f"[Node ID: {self.node_id}] Updating this node's successor's predecessor pointer to this node.")

                self_stub, self_channel = create_stub(
                    self.ip, self.port)

                with self_channel:
                    get_successor_request = chord_pb2.Empty()
                    get_successor_response = self_stub.GetSuccessor(
                        get_successor_request, timeout=5)
                    self.successor = Node(
                        get_successor_response.id, get_successor_response.ip, get_successor_response.port, self.m)

                successor_stub, successor_channel = create_stub(
                    self.successor.ip, self.successor.port)

                with successor_channel:
                    set_predecessor_request = chord_pb2.NodeInfo(
                        id=self.node_id, ip=self.ip, port=self.port)
                    successor_stub.SetPredecessor(
                        set_predecessor_request, timeout=5)

                logger.info(
                    f"[Node ID: {self.node_id}] Successfully updated the successor's predecessor pointer.")

                self_stub, self_channel = create_stub(
                    self.ip, self.port)

                with self_channel:
                    get_predecessor_request = chord_pb2.Empty()
                    get_predecessor_response = self_stub.GetPredecessor(
                        get_predecessor_request, timeout=5)
                    self.predecessor = Node(
                        get_predecessor_response.id, get_predecessor_response.ip, get_predecessor_response.port, self.m)

                logger.info(
                    f"[Node ID: {self.node_id}] Updating this node's predecessor's successor pointer to this node.")
                predecessor_stub, predecessor_channel = create_stub(
                    self.predecessor.ip, self.predecessor.port)

                with predecessor_channel:
                    set_successor_request = chord_pb2.NodeInfo(
                        id=self.node_id, ip=self.ip, port=self.port)
                    predecessor_stub.SetSuccessor(
                        set_successor_request, timeout=5)

                logger.info(
                    f"[Node ID: {self.node_id}] Successfully updated the predecessor's successor pointer.")

                logger.info(
                    f"[Node ID: {self.node_id}] Initializing hash table to get this node's keys.")
                self.initialize_store()
                logger.info(
                    f"[Node ID: {self.node_id}] Successfully initialized hash table.")

                logger.info(
                    f"[Node ID: {self.node_id}] Starting replication to successors.")
                self.replicate_keys_to_successor()
                logger.info(
                    f"[Node ID: {self.node_id}] Replication successful.")

            except Exception as e:
                logger.error(
                    f"[Node ID: {self.node_id}] Error joining the chord ring through bootstrap node: {bootstrap_node}")

    def calculate_finger_table_start(self, node_id, i) -> int:
        """
        Calculates the starting hash value for the ith entry in the finger table based on the given node's ID. This method 
        uses the formula defined in the Chord protocol to determine the start of the interval for which the ith entry is 
        responsible.

        Args:
            node_id (int): The identifier of the node for which the finger table is being calculated.
            i (int): The index of the entry in the finger table, where i ranges from 1 to m (the number of bits in the key space).

        Returns:
            int: The starting position in the node's key space for the ith entry of the finger table, computed as
                (node_id + 2^(i-1)) % 2^m. This value represents the smallest key for which the ith entry will be the successor.

        Explanation:
            The start value for each entry in the finger table is critical for determining the node's efficient routing 
            capabilities in the Chord network. Each entry in the finger table points to the successor of the start value,
            effectively covering different segments of the key space.
        """

        start = (node_id + (2 ** (i - 1))) % (2 ** self.m)
        return int(start)

    def initialize_finger_table(self, bootstrap_node):
        """
        Initializes the finger table of the node using the bootstrap node for initial network discovery. This method
        sets up the node's ability to efficiently locate successors for a range of keys by populating the finger table
        with correct entries from the network.

        Args:
            bootstrap_node (Node): The node used for bootstrapping into the network. This node provides the initial
                                point of contact to access the Chord ring and find successors for finger table entries.

        Steps:
            - Establishes the node's predecessor and the first entry of the finger table using the current successor.
            - Iteratively fills the finger table:
            - For each entry, checks if the start of the next finger falls between the current node and the last calculated
                finger's node. If true, copies the last finger's node to the next position.
            - Otherwise, queries the bootstrap node to find the appropriate successor for that finger start and updates
                the finger table accordingly.
            - Ensures that each entry in the finger table points to the closest node that precedes the start of the interval
            defined by that entry.

        Notes:
            This method is critical when a node first joins the network and needs to accurately reflect the network topology
            in its routing table (finger table) to participate effectively in the Chord ring.
        """
        successor = self.successor

        successor_stub, successor_channel = create_stub(
            successor.ip, successor.port)

        with successor_channel:
            get_predecessor_request = chord_pb2.Empty()
            get_predecessor_response = successor_stub.GetPredecessor(
                get_predecessor_request, timeout=5)
            self.predecessor = Node(
                get_predecessor_response.id, get_predecessor_response.ip, get_predecessor_response.port, self.m)

        self.finger_table[0] = self.successor

        for i in range(self.m-1):
            # finger_start = (self.node_id + 2**i) % (2**self.m)
            if is_within_bounds(self.calculate_finger_table_start(self.node_id, i+2), self.node_id, self.finger_table[i].node_id, 'l'):
                self.finger_table[i+1] = self.finger_table[i]

            else:
                bootstra_stub, bootstrap_channel = create_stub(
                    bootstrap_node.ip, bootstrap_node.port)
                with bootstrap_channel:
                    find_successor_request = chord_pb2.FindSuccessorRequest(
                        id=self.calculate_finger_table_start(self.node_id, i+2)
                    )
                    find_successor_response = bootstra_stub.FindSuccessor(
                        find_successor_request, timeout=5)
                    self.finger_table[i+1] = Node(find_successor_response.id,
                                                  find_successor_response.ip, find_successor_response.port, self.m)

    def calculate_preceding_id(self, current_id, offset) -> int:
        """
        Calculates the identifier that precedes the current node's ID by a specified offset within the Chord ring,
        taking into account the circular nature of the ring.

        This method is crucial for maintaining correct references in operations like updating finger tables and
        finding predecessors, especially in a distributed environment where node positions are dynamic and can wrap
        around the maximum ring size defined by `m`.

        Parameters:
            current_id (int): The current node's identifier.
            offset (int): The offset by which to go backward from the current node's ID, used to find preceding node IDs
                        in the Chord topology.

        Returns:
            int: The identifier of the node that precedes the current node by the given offset, correctly adjusted for
                wrap-around at the boundary of the ring.

        Example:
            If the current_id is 3, offset is 4, and m is set to 5 (ring size 2**5 = 32), then the method would return
            29, accounting for the wrap-around in the circular Chord ring.
        """

        potential_id = current_id - offset

        if potential_id >= 0:
            return potential_id
        else:
            return current_id + (2 ** self.m - offset)

    def update_other_nodes(self):
        """
        Notifies relevant nodes in the Chord network to update their finger tables to reflect the addition of this node.
        This method ensures that the finger tables throughout the network are accurate and include this node, thereby 
        maintaining the integrity and efficiency of the network routing.

        Steps:
            - Iterates through all entries in the node's own finger table.
            - For each entry, calculates the ID that would precede the node by 2^i positions in the ring.
            - Uses the node's stub to find the predecessor for each calculated ID.
            - Establishes a connection to each identified predecessor and sends a request to update its finger table
            to include this node.
            - Each update potentially involves modifying the finger table entry at index i of the predecessor node to
            point to this node if this node falls between the predecessor and its current entry at index i.
        """
        for i in range(self.m):
            update_id = self.calculate_preceding_id(self.node_id, 2**(i))
            stub, channel = create_stub(self.ip, self.port)

            with channel:
                find_pred_request = chord_pb2.FindPredecessorRequest(
                    id=update_id)
                find_pred_response = stub.FindPredecessor(find_pred_request)
                pred_ip = find_pred_response.ip
                pred_port = find_pred_response.port
                pred_id = find_pred_response.id
            pred_stub, pred_channel = create_stub(pred_ip, pred_port)

            with pred_channel:
                self_node_info = chord_pb2.NodeInfo(
                    id=self.node_id, ip=self.ip, port=self.port)

                update_finger_table_request = chord_pb2.UpdateFingerTableRequest(
                    node=self_node_info, i=i, for_leave=False)
                pred_stub.UpdateFingerTable(
                    update_finger_table_request, timeout=5)

    def fix_fingers(self):
        """
        Periodically refreshes a randomly selected entry in the finger table to maintain efficient routing within the Chord network.
        This method helps ensure that the finger table entries are up to date, reflecting any changes in the network topology
        and optimizing the node's ability to quickly locate successors for arbitrary keys.

        Steps:
            - Randomly selects an index in the finger table to update.
            - Calculates the start position for that finger based on the node's ID and the power of two corresponding to the index.
            - Makes a request to find the successor for that start position using the local node's stub.
            - Updates the finger table at the selected index with the new successor information.
        """

        i = random.randint(0, self.m - 1)
        finger_start = (self.node_id + 2**i) % (2**self.m)
        stub, channel = create_stub(self.ip, self.port)
        try:
            with channel:
                find_successor_request = chord_pb2.FindSuccessorRequest(
                    id=finger_start)
                find_successor_response = stub.FindSuccessor(
                    find_successor_request, timeout=5)
                self.finger_table[i] = Node(find_successor_response.id,
                                            find_successor_response.ip,
                                            find_successor_response.port, self.m)
        except Exception as e:
            return

    def stabilize(self):
        """
        Periodically updates this node's successor link to ensure the network remains coherent and efficient. This method
        is part of the stabilization process in the Chord DHT, which helps correct and update successor pointers to ensure
        the ring maintains proper data responsibility and routing.

        Steps:
            - Establishes a gRPC connection to the current node's own server.
            - Sends a 'SetSuccessor' request to itself to potentially update its successor based on the latest network state.
            - This is typically used to confirm or adjust the successor if changes have occurred in the network topology.
        """
        try:
            stub, channel = create_stub(self.ip, self.port)
            with channel:
                set_successor_request = chord_pb2.NodeInfo(
                    id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port)
                stub.SetSuccessor(set_successor_request, timeout=5)
        except Exception:
            return

    def initialize_store(self):
        """
        Initializes the local store by retrieving data from the successor node that is relevant to this node's key range.
        This method is typically called when a new node joins the network and needs to take over responsibility for specific keys.

        Steps:
            - Establishes a connection to the successor node using a gRPC stub.
            - Sends a 'GetTransferDataRequest' to the successor to fetch all key-value pairs that should now be managed by this node.
            - Receives and deserializes the data from the successor, and initializes the local store with this data.
        """

        successor_stub, successor_channel = create_stub(
            self.successor.ip, self.successor.port)
        with successor_channel:
            get_transfer_data_request = chord_pb2.GetTransferDataRequest(
                id=self.node_id)
            get_transfer_data_response = successor_stub.GetTransferData(
                get_transfer_data_request, timeout=5)

            data = get_transfer_data_response.data
        self.store = ast.literal_eval(data)

    def set(self, key, filename):
        """
        Stores or updates a key-value pair in the network by determining the appropriate successor node for the key
        and sending the data to be stored at that node. This method is essential for distributing and maintaining
        data across the Chord DHT.

        Args:
            key (str): The key under which the data should be stored.
            filename (str): The data or filename associated with the key to be stored.

        Returns:
            chord_pb2.SetKeyResponse: A protobuf object containing the response from the successor node, indicating
                                    the success or failure of the set operation.

        Steps:
            - Hashes the key to find its correct position within the Chord ring.
            - Finds the successor node responsible for the hashed key using a 'FindSuccessor' request.
            - Sends a 'SetKey' request to the identified successor node with the data to be stored.
            - Returns the response from the successor node, which can include confirmation of data storage or an error message.
        """
        hashed_key = sha1_hash(key, self.m)
        stub, channel = create_stub(self.ip, self.port)
        with channel:
            find_successor_request = chord_pb2.FindSuccessorRequest(
                id=hashed_key)
            find_successor_response = stub.FindSuccessor(
                find_successor_request, timeout=5)

        successor_stub, successor_channel = create_stub(
            find_successor_response.ip, find_successor_response.port)

        with successor_channel:
            set_key_request = chord_pb2.SetKeyRequest(
                key=hashed_key, filename=filename
            )
            set_key_response = successor_stub.SetKey(
                set_key_request, timeout=5)

        return set_key_response

    def get(self, key):
        """
        Retrieves the successor node responsible for a given key by computing the key's hash and querying the network
        to find which node is responsible for that hash. This method is a fundamental operation in the Chord DHT,
        facilitating data retrieval across the distributed network.

        Args:
            key (str): The key for which the successor node needs to be located.

        Returns:
            chord_pb2.NodeInfo: A protobuf object containing the successor node's information, including node ID, IP address,
                                and port, indicating where the data associated with the key can be accessed.

        Steps:
            - Hashes the key to determine its position in the Chord ring.
            - Connects to the local node's stub to send a 'FindSuccessor' request based on the hashed key.
            - Returns the response from the network which includes the details of the successor node responsible for the key.
        """
        
        hashed_key = sha1_hash(key, self.m)
        stub, channel = create_stub(self.ip, self.port)
        with channel:
            find_successor_request = chord_pb2.FindSuccessorRequest(
                id=hashed_key)
            find_successor_response = stub.FindSuccessor(
                find_successor_request, timeout=5)
        return find_successor_response

    def receive_keys_before_leave(self, store):
        """
        Updates the local store with keys received from a departing node. This method is typically called during
        the process of a node's graceful exit from the network, where it transfers its responsibilities (keys and associated
        data) to this node.

        Args:
            store (dict): A dictionary containing key-value pairs where each key is accompanied by its metadata (ownership status).
                        The keys in this dictionary are those for which this node will take responsibility.

        Steps:
            - Iterates through each key in the provided store.
            - Updates the local store to reflect the new ownership status of each key.

        Note:
            This method is crucial for maintaining data integrity and availability in the network by ensuring that the
            departing node's data is seamlessly transferred and integrated into this node's local store.
        """

        for key in store:
            self.store[key][0] = store[key][0]

    def replicate_keys_to_successor(self, store=None):
        """
        Replicates key-value pairs to successor nodes to ensure data redundancy and integrity within the Chord network. 
        This method supports both full store replication (default behavior) or selective replication if a specific subset
        of the store is provided.

        Args:
            store (dict, optional): A dictionary of key-value pairs to be replicated. If None, the method replicates 
                                    all owned keys marked as true (i.e., self.store[key][0] is True) from the node's local store.

        Steps:
            - Iterates over each successor in the successor list.
            - For each successor, establishes a gRPC connection and sends the keys either from the provided store 
            or from the built store containing all owned keys.
            - Each key's associated data file is also uploaded to ensure the successor has the complete data set.
        Notes:
            This function is critical for maintaining data availability and is especially utilized during node departures
            or when redistributing responsibilities within the network.
        """
            
        try:
            for i, successor in enumerate(self.successor_list):
                if not store:
                    build_store = {}
                    for key in self.store:
                        if self.store[key][0]:
                            build_store[key] = [False, self.store[key][1]]
                    successor_stub, successor_channel = create_stub(
                        successor.ip, successor.port)
                    with successor_channel:
                        receive_keys_before_leave_request = chord_pb2.ReceiveKeysBeforeLeaveRequest(
                            store=str(build_store)
                        )
                        successor_stub.ReceiveKeysBeforeLeave(
                            receive_keys_before_leave_request, timeout=5)

                        for key in build_store:
                            successor_stub.UploadFile(
                                generate_requests(build_store[key][1]), timeout=5)
                else:
                    successor_stub, successor_channel = create_stub(
                        successor.ip, successor.port)
                    with successor_channel:
                        receive_keys_before_leave_request = chord_pb2.ReceiveKeysBeforeLeaveRequest(
                            store=str(store)
                        )
                        successor_stub.ReceiveKeysBeforeLeave(
                            receive_keys_before_leave_request, timeout=5)

                        for key in store:
                            successor_stub.UploadFile(
                                generate_requests(store[key][1]), timeout=5)
        except Exception as e:
            pass

    def replicate_single_key_to_successor(self, key):
        """
        Specifically replicates a single key-value pair to this node's successor. This method is typically used to 
        ensure data integrity and availability when updates occur that affect a single key, or when a key needs to 
        be explicitly reassured due to node changes or other operational requirements.

        Args:
            key (str): The key of the data entry to be replicated to the successor.

        Steps:
            - Prepares the key-value pair from the local store, marking it as not directly owned (False) before sending.
            - Calls the general `replicate_keys_to_successor` method to handle the actual replication process, passing 
            it the prepared dictionary containing just this one key-value pair.

        Notes:
            This method is a specialized use case of the more general `replicate_keys_to_successor`, optimized for 
            handling single key replication efficiently without needing to process the entire store.
        """

        store = {key: [False, self.store[key][1]]}
        self.replicate_keys_to_successor(store)

    def transfer_before_leave(self):
        """
        Handles the transfer of keys and associated data to this node's designated successor in preparation for 
        the node's exit from the network. This method ensures that all data managed by this node is safely 
        transitioned to another node, maintaining the integrity and availability of data within the Chord DHT.

        The method performs two main actions:
        - Transfers the key-value pairs (the 'store') to the successor.
        - Initiates the transfer of file data to the successor, ensuring that all files this node is responsible for 
        are securely moved to the next node in the Chord ring.
        """

        successor_stub, successor_channel = create_stub(
            self.successor.ip, self.successor.port)
        with successor_channel:
            receive_keys_before_leave_request = chord_pb2.ReceiveKeysBeforeLeaveRequest(
                store=str(self.store)
            )
            successor_stub.ReceiveKeysBeforeLeave(
                receive_keys_before_leave_request, timeout=5)

        self.transfer_files_before_leave()

    def leave(self):
        """
        Facilitates the graceful departure of this node from the Chord DHT network. This method ensures that all necessary 
        updates are made to maintain the network's integrity and data consistency before the node officially leaves. 
        It updates both the predecessor and successor nodes' references and redistributes the responsibilities and files 
        this node was handling.

        Steps:
            - Notifies this node's predecessor to set its successor to this node's current successor.
            - Notifies this node's successor to set its predecessor to this node's current predecessor.
            - Updates the predecessor's finger table to reflect the changes.
            - Transfers all files this node is responsible for to the appropriate successor to ensure no data is lost.
            - Logs significant actions to provide a clear trail of operations performed during the node's departure.
        """

        logger.info(f"[Node ID: {self.node_id}] Starting to leave the system.")
        logger.info(
            f"[Node ID: {self.node_id}] Setting predecessor's [{self.predecessor}] successor to this node's successor [{self.successor}].")
        predecessor_stub, predecessor_channel = create_stub(
            self.predecessor.ip, self.predecessor.port)
        with predecessor_channel:
            set_successor_request = chord_pb2.NodeInfo(
                id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port)
            predecessor_stub.SetSuccessor(set_successor_request, timeout=5)
        logger.info(
            f"[Node ID: {self.node_id}] Setting successor's [{self.successor}] predecessor to this node's predecessor [{self.predecessor}].")
        successor_stub, successor_channel = create_stub(
            self.successor.ip, self.successor.port)
        with successor_channel:
            set_predecessor_request = chord_pb2.NodeInfo(
                id=self.predecessor.node_id, ip=self.predecessor.ip, port=self.predecessor.port)
            successor_stub.SetPredecessor(set_predecessor_request, timeout=5)

        predecessor_stub, predecessor_channel = create_stub(
            self.predecessor.ip, self.predecessor.port)
        with predecessor_channel:
            nodeinfo = chord_pb2.NodeInfo(
                id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port)
            update_finger_table_request = chord_pb2.UpdateFingerTableRequest(
                node=nodeinfo, i=0, for_leave=True)
            predecessor_stub.UpdateFingerTable(
                update_finger_table_request, timeout=5)

        logger.info(
            f"[Node ID: {self.node_id}] Transferring all the files to appropriate node.")
        self.transfer_before_leave()

        logger.info(
            f"[Node ID: {self.node_id}] Node {self.ip}:{self.port} left the system successfully.")

    def upload_file(self, file_path):
        """
        Uploads a file to the appropriate node in the Chord network based on the hashed key of the file path. This method
        handles the initial key setting and then locates the correct successor node to store the file. It ensures the file 
        is uploaded to the node responsible for the hash range into which the file's key falls.

        Args:
            file_path (str): The path to the file that needs to be uploaded. The path is used to generate a hash key.

        Returns:
            chord_pb2.UploadFileResponse: A response from the successor node indicating the success or failure of the file upload.

        Steps:
            - The file path is hashed to determine which node should store the file.
            - A connection is established to this node, and a request is made to upload the file.
            - The method returns the response from the target node, providing feedback on the upload process.
        """

        hashed_key = sha1_hash(file_path, self.m)
        stub, channel = create_stub(self.ip, self.port)
        with channel:
            set_key_request = chord_pb2.SetKeyRequest(
                key=hashed_key, filename=file_path)
            set_key_response = stub.SetKey(set_key_request, timeout=5)
            find_successor_request = chord_pb2.FindSuccessorRequest(
                id=hashed_key)
            find_successor_response = stub.FindSuccessor(
                find_successor_request, timeout=5)

        target_node_stub, target_node_channel = create_stub(
            find_successor_response.ip, find_successor_response.port)
        
        with target_node_channel:
            upload_file_response = target_node_stub.UploadFile(
                generate_requests(file_path), timeout=5)

        return upload_file_response

    def show_store(self):
        """
        Displays the contents of the node's local storage as a table. This method is used to visually inspect 
        the files or data entries currently stored on this node within the Chord Distributed Hash Table network.

        Each entry in the node's store is listed with a sequential number and the corresponding file name or data identifier.
        This function is particularly useful for debugging and administrative purposes, providing a clear and organized 
        view of the stored data.

        Output:
            The method prints a formatted table to the console that includes columns for the entry number and file name.
        """

        table = [["No.", "File Name"]]
        for i, key in enumerate(self.store, start=1):
            table.append([i, self.store[key][1]])

        tab = PrettyTable(table[0])
        tab.add_rows(table[1:])
        tab.hrules = ALL
        print(tab)

    def show_finger_table(self):
        """
        Displays the current node's finger table in a tabular format. The finger table is essential for efficient
        routing in the Chord Distributed Hash Table network, containing links to other nodes in the system.

        This method constructs and prints a table showing the index, the start value of the hash interval, 
        and the successor node ID for each entry in the finger table. It helps in debugging and understanding 
        the network topology at a glance.

        Output:
            The method prints a table to the console with columns for the index 'i', the 'Start' of the range each 
            entry covers, and the 'Successor' node ID responsible for that range.
        """
        table = [["i", "Start", "Successor"]]
        for i in range(self.m):
            table.append([i, self.calculate_finger_table_start(self.node_id, i),
                         self.finger_table[i].node_id])

        tab = PrettyTable(table[0])
        tab.add_rows(table[1:])
        tab.hrules = ALL
        print(tab)

    def perform_gossip(self, message_id, message):
        """
        Disseminates a gossip message across the network to maintain and update distributed system state or data. 
        This method ensures that all relevant nodes in the network receive updates or critical notifications 
        through a controlled flood using the node's finger table to spread the information efficiently.

        Args:
            message_id (str): A unique identifier for the gossip message to prevent re-broadcasting of the same message.
            message (str): The content of the gossip message to be distributed across the network.

        Steps:
            - If the message ID is new, it adds it to the received messages and starts the gossip process.
            - The message is sent to all unique nodes in the node's finger table except itself.
            - Logs each gossip transmission to track which nodes have been informed.
        """
        
        unique_nodes = {}
        if message_id not in self.received_gossip_message_ids:
            self.received_gossip_message_ids.add(message_id)
            self.received_gossip_messages.append(message)
            # unique_nodes[self.node_id] = self

        for i in range(self.m):
            unique_nodes[self.finger_table[i].node_id] = self.finger_table[i]

        unique_nodes.pop(self.node_id, None)

        for node in unique_nodes:
            node_stub, node_channel = create_stub(
                unique_nodes[node].ip, unique_nodes[node].port)
            with node_channel:
                gossip_request = chord_pb2.GossipRequest(
                    message=message, message_id=message_id)
                node_stub.Gossip(gossip_request, timeout=5)
                print(
                    f"[Node ID: {self.node_id}] Gossip message sent to node {unique_nodes[node].ip}:{unique_nodes[node].port}")
                logger.info(
                    f"[Node ID: {self.node_id}] Gossip message sent to node {unique_nodes[node].ip}:{unique_nodes[node].port}")

    def transfer_files_before_leave(self):
        """
        Transfers all files stored on this node to the appropriate successors before the node leaves the network. 
        This method ensures that no data is lost when a node departs, maintaining the integrity and availability 
        of data across the Chord DHT.

        Each file is sent to the next successor in line who will be responsible for the keys that the departing node 
        held. This method handles both setting the new key-owner and uploading the file content to the successor nodes.
        """

        files_to_transfer = []
        for key in self.store:
            files_to_transfer.append([key, self.store[key][1]])

        for i, successor in enumerate(self.successor_list):

            successor_stub, successor_channel = create_stub(
                successor.ip, successor.port)

            with successor_channel:

                for file_key, file_path in files_to_transfer:
                    set_key_request = chord_pb2.SetKeyRequest(
                        key=file_key, filename=file_path)
                    set_key_response = successor_stub.SetKey(
                        set_key_request, timeout=5)

                    upload_file_response = successor_stub.UploadFile(
                        generate_requests(file_path), timeout=5)

                    logger.info(
                        f"[Node ID: {self.node_id}] Transferred file {file_path} to node with ip: {successor.ip} and port: {successor.port}")
