"""
This module provides RPC service handling for Chord protocol operations such as joining the network, finding successors and predecessors, 
and stabilizing the network. The module leverages gRPC for inter-node communication and implements 
necessary functionalities to maintain and query the distributed hash table efficiently.

Classes:
    Node: Manages node-specific data such as the finger table, successors, predecessors, and key-value store.
    ChordNodeServicer: Inherits from the gRPC-generated ChordServiceServicer and handles RPC calls,
                       ensuring proper Chord protocol operations such as node join, stabilize, and key management.

Usage:
    The module can be executed directly to start a Chord node in a standalone or networked environment. 
    It uses command-line arguments to configure the node's IP, port, and bootstrap connection details.

Example:
    To run a Chord node that connects to an existing network:
    $ python chord_server.py --ip 192.168.1.5 --port 50051 --bootstrap_ip 192.168.1.1 --bootstrap_port 50050

Dependencies:
    grpc: For implementing the network layer.
    proto (chord_pb2, chord_pb2_grpc): Protocol buffers for defining and serializing structured data.
    threading, futures: For concurrent operations.
    ast, os, uuid, time: For various utility functions like parsing strings, file operations, unique ID generation, etc.
    utils: Includes helper functions like sha1_hash for hashing and create_stub for creating gRPC stubs.
    constants: Contains constants like M (hash space size), SUCCESSOR_COUNT, etc.

Notes:
    Ensure all dependencies are installed and proper firewall and network configurations are set 
    to allow TCP connections on the designated ports for gRPC communication.
"""

import grpc
from proto import chord_pb2
from proto import chord_pb2_grpc
import threading
from concurrent import futures
import ast
import os
from utils import sha1_hash, get_args, create_stub, is_within_bounds, download_file, menu_options, load_ssl_credentials, logger
from constants import M, SUCCESSOR_COUNT, STABALIZATION_INTERVAL, STORE_DIR, DOWNLOAD_DIR
from node import Node
import time
import uuid

os.environ['GRPC_VERBOSITY'] = 'NONE'

def run_stabilization(node):
    """
    Executes stabilization and finger table maintenance in a loop for the specified node.

    This function is intended to run in a separate thread to continuously update the node's state 
    in the background, addressing changes within the Chord DHT network. It handles any exceptions 
    by logging them and continues running.

    Args:
        node (Node): The node on which stabilization and finger table updates are performed.

    Returns:
        None: Designed to run indefinitely; does not return a value.

    Example:
        # Assuming `node_instance` is an instance of `Node`:
        stabilization_thread = threading.Thread(target=run_stabilization, args=(node_instance,))
        stabilization_thread.start()
    """

    while True:
        try:
            node.stabilize()
            node.fix_fingers()
        except Exception as e:
            logger.exception(
                f"[Node ID: {node.node_id}] Error in stabilization process")
        time.sleep(STABALIZATION_INTERVAL)


class ChordNodeServicer(chord_pb2_grpc.ChordServiceServicer):
    """
    Class to handle RPC calls for Chord protocol operations.
    """

    def __init__(self, node: Node):
        self.node = node

    def GetSuccessor(self, request, context):
        """
        Responds with the current node's successor information as a NodeInfo object.
        This method is typically called by other nodes in the Chord network or by client applications
        needing to navigate the Chord ring.

        Args:
            request: gRPC request object, not used here as no additional data is required.
            context: gRPC context object used to handle the RPC environment.

        Returns:
            chord_pb2.NodeInfo: Contains the successor's node ID, IP address, and port.

        """

        try:
            response = chord_pb2.NodeInfo()
            response.id = self.node.successor.node_id
            response.ip = self.node.successor.ip
            response.port = self.node.successor.port
            return response
        except Exception as e:
            logger.exception(
                f"[Node ID: {self.node.node_id}] Error while getting successor.")

    def SetSuccessor(self, request, context):
        """
        Updates the successor information for the current node based on the provided request.

        This method sets the immediate successor of the node and updates the node's successor list
        to reflect changes in the network topology. If any node in the successor list fails, it adjusts
        the list and tries to maintain stability in the Chord ring by connecting with the next valid successor.

        Args:
            request (chord_pb2.NodeInfo): Contains the new successor's ID, IP, and port.
            context (grpc.ServicerContext): The context for the current RPC call.

        Updates:
            Updates the node's successor and the successor list up to SUCCESSOR_COUNT.
        
        Returns:
            chord_pb2.Empty: A protobuf object indicating successful processing of the request.
        """

        id = request.id
        ip = request.ip
        port = request.port
        self.node.successor = Node(id, ip, port, self.node.m)
        self.node.successor_list[0] = self.node.successor

        i = 1
        while i < SUCCESSOR_COUNT: # Update the successor list
            intermediate_node = self.node.successor_list[i-1]
            try:
                if intermediate_node is not None:
                    intermediate_stub, intermediate_channel = create_stub(
                        intermediate_node.ip, intermediate_node.port)
                    with intermediate_channel:
                        get_successor_request = chord_pb2.Empty()
                        get_successor_response = intermediate_stub.GetSuccessor(
                            get_successor_request)
                        self.node.successor_list[i] = Node(
                            get_successor_response.id, get_successor_response.ip, get_successor_response.port, self.node.m)
            except Exception as e: # If the node is unreachable, suspect a crash and update the successor list
                self.node.successor_list[i-1] = self.node.successor_list[i]
                if i == 1: # If the first node in the list fails, update the successor

                    print(f"Suspect a crash for node {self.node.successor}")
                    logger.warning(
                        f"[Node ID: {self.node.node_id}] Suspect a crash for node {self.node.successor}.")
                    self.node.successor = self.node.successor_list[i]

                    self_stub, self_channel = create_stub(
                        self.node.ip, self.node.port)
                    with self_channel:
                        update_finger_table_request = chord_pb2.UpdateFingerTableRequest(
                            node=chord_pb2.NodeInfo(id=self.node.successor.node_id, ip=self.node.successor.ip, port=self.node.successor.port), i=0, for_leave=True)

                    try:
                        successor_stub, successor_channel = create_stub(
                            self.node.successor.ip, self.node.successor.port)
                        with successor_channel:
                            set_predecessor_request = chord_pb2.NodeInfo(
                                id=self.node.node_id, ip=self.node.ip, port=self.node.port)
                            successor_stub.SetPredecessor(
                                set_predecessor_request)
                    except Exception as e:
                        pass

                    self.node.replicate_keys_to_successor() # Replicate keys to the new successor
            i += 1

        return chord_pb2.Empty()

    def GetPredecessor(self, request, context):
        """
        Retrieves the predecessor information of the current node and returns it as a NodeInfo object.
        This RPC method is crucial for maintaining the Chord ring integrity, allowing nodes to discover
        and verify their immediate predecessor in the network.

        Args:
            request (chord_pb2.Empty): An empty request object used to invoke this method.
            context (grpc.ServicerContext): The RPC context, providing runtime methods and properties for this RPC.

        Returns:
            chord_pb2.NodeInfo: A protobuf object containing the predecessor's node ID, IP address, and port.
        """

        try:
            response = chord_pb2.NodeInfo()
            response.id = self.node.predecessor.node_id
            response.ip = self.node.predecessor.ip
            response.port = self.node.predecessor.port

            return response
        except Exception as e:
            logger.exception(
                f"[Node ID: {self.node.node_id}] Error while getting predecessor.")

    def SetPredecessor(self, request, context):
        """
        Sets the predecessor of the current node using the information provided in the request. This method 
        is vital for maintaining the correct topology of the Chord ring, particularly when nodes join or leave.

        Args:
            request (chord_pb2.NodeInfo): The request object containing the new predecessor's ID, IP, and port.
            context (grpc.ServicerContext): The RPC context, which provides runtime methods and properties for this RPC.

        Returns:
            chord_pb2.Empty: Returns an empty protobuf object indicating the successful execution of the method.
        """

        try:
            id = request.id
            ip = request.ip
            port = request.port
            self.node.predecessor = Node(id, ip, port, self.node.m)
            return chord_pb2.Empty()
        except Exception as e:
            logger.exception(
                f"[Node ID: {self.node.node_id}] Error while setting predecessor.")

    def FindPredecessor(self, request, context):
        """
        Locates the predecessor of the specified node ID within the Chord ring. This RPC method is crucial for 
        maintaining the Chord DHT structure by facilitating efficient key lookups and node insertions.

        Args:
            request (chord_pb2.FindPredecessorRequest): The request object containing the node ID for which the 
                                                        predecessor is sought.
            context (grpc.ServicerContext): The RPC context, providing runtime methods and properties for the RPC.

        Returns:
            chord_pb2.NodeInfo: A protobuf object containing the predecessor's node ID, IP address, and port.
            This response is crucial for client nodes to correctly route queries or integrate into the Chord ring.
        """

        try:
            if self.node.successor.node_id == self.node.node_id:
                response = chord_pb2.NodeInfo()
                response.id = self.node.node_id
                response.ip = self.node.ip
                response.port = self.node.port
                return response

            else:
                id_to_find = request.id
                current_node = self.node
                # Find the predecessor of the node with the given ID
                while not (is_within_bounds(id_to_find, current_node.node_id, current_node.successor.node_id, 'right_open')):
                    current_node_stub, current_node_channel = create_stub(
                        current_node.ip, current_node.port)

                    # Find the closest preceding finger to the given ID
                    with current_node_channel:
                        find_closest_preceding_finger_request = chord_pb2.FindClosestPrecedingFingerRequest(
                            id=id_to_find)
                        closest_resp = current_node_stub.FindClosestPrecedingFinger(
                            find_closest_preceding_finger_request)
                        current_node = Node(
                            closest_resp.id, closest_resp.ip, closest_resp.port, self.node.m)

                    current_node_stub, current_node_channel = create_stub(
                        current_node.ip, current_node.port)
                    with current_node_channel:
                        get_successor_request = chord_pb2.Empty()
                        get_successor_response = current_node_stub.GetSuccessor(
                            get_successor_request)
                        current_node.successor = Node(
                            get_successor_response.id, get_successor_response.ip, get_successor_response.port, self.node.m)

                response = chord_pb2.NodeInfo()
                response.id = current_node.node_id
                response.ip = current_node.ip
                response.port = current_node.port
                return response

        except Exception:
            logger.error(
                f"[Node ID: {self.node.node_id}] Error while finding predecessor.")

    def FindSuccessor(self, request, context):
        """
        Retrieves the successor of the specified node ID within the Chord ring. This method is critical
        for operations such as node join and key lookup, ensuring that queries and data are correctly routed
        according to the Chord protocol.

        Args:
            request (chord_pb2.FindSuccessorRequest): The request object containing the ID for which the
                                                    successor needs to be located.
            context (grpc.ServicerContext): The RPC context, providing runtime methods and properties for
                                            managing the RPC environment.

        Returns:
            chord_pb2.NodeInfo: A protobuf object containing the successor's node ID, IP address, and port,
                                which is critical for correctly forwarding or handling the Chord operations.
        """

        try:
            id_to_find = request.id

            node_stub, node_channel = create_stub(self.node.ip, self.node.port)
            with node_channel:
                find_pred_request = chord_pb2.FindPredecessorRequest(
                    id=id_to_find)
                find_pred_response = node_stub.FindPredecessor(
                    find_pred_request)

            pred_stub, pred_channel = create_stub(
                find_pred_response.ip, find_pred_response.port)

            with pred_channel:
                get_succ_request = chord_pb2.Empty()
                get_succ_response = pred_stub.GetSuccessor(get_succ_request)

            return get_succ_response
        except Exception as e:
            logger.error(
                f"[Node ID: {self.node.node_id}] Error while finding successor.")

    def FindClosestPrecedingFinger(self, request, context):
        """
        Identifies the closest preceding finger in the Chord ring relative to a given node ID. This method
        aids in efficiently navigating the Chord ring to locate the appropriate node for forwarding a request or
        performing a key lookup.

        Args:
            request (chord_pb2.FindClosestPrecedingFingerRequest): The request object containing the node ID 
                                                                for which the closest preceding finger is sought.
            context (grpc.ServicerContext): Provides RPC-specific information (like timeout and cancellation)
                                            and allows the insertion of custom metadata into response headers.

        Returns:
            chord_pb2.NodeInfo: A protobuf object that represents the closest node (finger) preceding the
                                specified node ID. Contains node ID, IP address, and port.
        """

        try:
            id_to_find = request.id
            response = chord_pb2.NodeInfo()
            for i in range(self.node.m-1, -1, -1):
                if is_within_bounds(self.node.finger_table[i].node_id, self.node.node_id, id_to_find, 'closed'):
                    response.id = self.node.finger_table[i].node_id
                    response.ip = self.node.finger_table[i].ip
                    response.port = self.node.finger_table[i].port
                    return response

            response.id = self.node.node_id
            response.ip = self.node.ip
            response.port = self.node.port
            return response
        except Exception as e:
            logger.error(
                f"[Node ID: {self.node.node_id}] Error while finding closest preceding finger.")

    def UpdateFingerTable(self, request, context):
        """
        Updates the finger table of the current node based on the information provided in the request.
        This method is critical for maintaining the accuracy of the Chord routing table, especially when
        nodes join or leave the network.

        Args:
            request (chord_pb2.UpdateFingerTableRequest): The request object containing the index 'i' at which to update the finger table,
                                                        the node information to be used for the update, and a flag indicating whether
                                                        the update is due to a node leaving.
            context (grpc.ServicerContext): The RPC context, which provides runtime methods and properties for handling the RPC.

        Returns:
            chord_pb2.Empty: Returns an empty protobuf object indicating successful processing of the request.
        """

        i = request.i
        source_node = request.node
        source_id = source_node.id
        source_ip = source_node.ip
        source_port = source_node.port
        for_leave = request.for_leave
        if for_leave:
            self.node.finger_table[i] = Node(
                source_id, source_ip, source_port, self.node.m)
            return chord_pb2.Empty()

        if source_id != self.node.node_id and is_within_bounds(source_id, self.node.node_id, self.node.finger_table[i].node_id, 'left_open'):
            self.node.finger_table[i] = Node(
                source_id, source_ip, source_port, self.node.m)
            pred = self.node.predecessor

            while True: # Update the predecessor if it is the current node
                try:
                    if not pred or pred.node_id == self.node.node_id:
                        pred = self
                    else:
                        pred_stub, pred_channel = create_stub(
                            pred.ip, pred.port)
                        with pred_channel:
                            source_node_info = chord_pb2.NodeInfo()
                            source_node_info.id = source_id
                            source_node_info.ip = source_ip
                            source_node_info.port = source_port

                            update_finger_table_request = chord_pb2.UpdateFingerTableRequest(
                                i=i, node=source_node_info, for_leave=False)
                            pred_stub.UpdateFingerTable(
                                update_finger_table_request)
                    break
                except Exception as e:
                    logger.error(
                        f"[Node ID: {self.node.node_id}] Encountered some error while updating the finger table: {e}")
                    continue

        return chord_pb2.Empty()

    def GetTransferData(self, request, context):
        """
        Retrieves data that should be transferred to a newly joining node as part of the Chord DHT stabilization process.
        This method identifies which parts of the current node's data store should be moved to the new node based on the 
        new node's identifier.

        Args:
            request (chord_pb2.GetTransferDataRequest): Contains the identifier of the new node that will receive the data.
            context (grpc.ServicerContext): Provides RPC-specific methods and properties, assisting in managing the request.

        Returns:
            chord_pb2.GetTransferDataResponse: Contains the data to be transferred, formatted as a string dictionary.
        """

        try:
            node_id = request.id
            transfer_data = {}
            keys_to_be_deleted = []

            for key in self.node.store:
                if is_within_bounds(key, self.node.node_id, node_id, 'o'):
                    transfer_data[key] = [True, self.node.store[key][1]]
                    keys_to_be_deleted.append(key)

            for key in keys_to_be_deleted:
                del self.node.store[key]

            response = chord_pb2.GetTransferDataResponse()
            response.data = str(transfer_data)
            return response
        except Exception as e:
            logger.error(
                f"[Node ID: {self.node.node_id}] Error while getting transfer data.")

    def SetKey(self, request, context):
        """
        Sets or updates a key-value pair in the node's local store. This method is essential for maintaining the 
        distributed hash table's integrity, allowing nodes to directly store or update data as part of the Chord 
        DHT's distributed storage mechanism.

        Args:
            request (chord_pb2.SetKeyRequest): The request object containing the key and the corresponding value 
                                            (typically a filename or data identifier) to be stored.
            context (grpc.ServicerContext): The RPC context, which provides runtime methods and properties for 
                                            managing the RPC and responding to the client.

        Returns:
            chord_pb2.NodeInfo: A protobuf object containing this node's ID, IP address, and port, which confirms 
                                the node that now holds the key.
        """

        try:
            key = request.key
            self.node.store[key] = [True, request.filename]
            if self.node.node_id != self.node.successor.node_id:
                self.node.replicate_single_key_to_successor(key)

            return chord_pb2.NodeInfo(id=self.node.node_id, ip=self.node.ip, port=self.node.port)
        except Exception as e:
            logger.error(
                f"[Node ID: {self.node.node_id}] Error while setting key.")

    def GetKey(self, request, context):
        """
        Retrieves the value associated with a specified key from the node's store. This method is crucial for 
        accessing stored data in the Chord DHT, facilitating data retrieval operations across the network.

        Args:
            request (chord_pb2.GetKeyRequest): The request object containing the key for which the value 
                                            (or associated data) is sought.
            context (grpc.ServicerContext): Provides RPC-specific methods and properties, helping manage the
                                            request and response lifecycle.

        Returns:
            chord_pb2.NodeInfo: A protobuf object containing this node's ID, IP address, and port if the key
                                is found, indicating the location of the data. Returns an empty NodeInfo if the 
                                key is not found.
        """

        try:
            key = request.key
            if key in self.node.store:
                return chord_pb2.NodeInfo(id=self.node.node_id, ip=self.node.ip, port=self.node.port)
            else:
                return chord_pb2.NodeInfo(id=None, ip=None, port=None)
        except Exception as e:
            logger.error(
                f"[Node ID: {self.node.node_id}] Error while getting key.")

    def ReceiveKeysBeforeLeave(self, request, context):
        """
        Handles the reception of keys from a node that is preparing to leave the network. This method 
        ensures that the departing node's data is securely transferred to this node, maintaining data
        integrity and availability in the Chord DHT.

        Args:
            request (chord_pb2.ReceiveKeysBeforeLeaveRequest): Contains serialized data representing the 
                                                            keys and their associated values to be transferred.
            context (grpc.ServicerContext): Provides RPC-specific methods and properties, assisting in the
                                            management of the request and ensuring a proper response.

        Returns:
            chord_pb2.Empty: Returns an empty protobuf object, indicating successful reception and storage
                            of the transferred data.
        """
        
        try:
            store_received = ast.literal_eval(request.store)

            for key in store_received:
                self.node.store[key] = store_received[key]

            return chord_pb2.Empty()
        except Exception as e:
            logger.error(
                f"[Node ID: {self.node.node_id}] Error while receiving keys before leave.")

    def DownloadFile(self, request, context):
        """
        Streams a file from the node's local storage to the requester. This method enables efficient 
        file downloads via the Chord DHT by reading and transmitting the file in chunks, ensuring that 
        large files can be handled without excessive memory usage.

        Args:
            request (chord_pb2.DownloadFileRequest): The request object containing the filename of the 
                                                    file to be downloaded.
            context (grpc.ServicerContext): Provides RPC-specific methods and properties, facilitating 
                                            the management of the RPC and error handling.

        Yields:
            chord_pb2.DownloadFileResponse: Streams back chunks of the requested file as a sequence of 
                                            messages, each containing a part of the file.
        """

        try:
            file_name = request.filename
            file_name = os.path.join(STORE_DIR, file_name)
            with open(file_name, 'rb') as f:
                while True:
                    chunk = f.read(1024)
                    if not chunk:
                        break
                    yield chord_pb2.DownloadFileResponse(buffer=chunk)
        except Exception as e:
            print(f"Error downloading file: {e}")
            logger.warning(
                f"[Node ID: {self.node.node_id}] Error downloading file: {e}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('File not found')
            return

    def UploadFile(self, request_iterator, context):
        """
        Receives a file uploaded by a client and stores it in the node's local storage. This method handles file
        uploads in chunks to accommodate large files without consuming excessive memory. Each chunk is received
        sequentially, and the file is reconstructed and stored on the server.

        Args:
            request_iterator (Iterable[chord_pb2.UploadFileRequest]): An iterator of request objects, each containing
                                                                    a chunk of the file and the filename on the first request.
            context (grpc.ServicerContext): Provides RPC-specific methods and properties, aiding in the management
                                            of the RPC and handling any errors that may occur.

        Returns:
            chord_pb2.UploadFileResponse: Returns a message indicating the success of the upload or an error message
                                        if the upload fails.
        """

        try:
            file_path = None
            for request in request_iterator:
                if file_path is None:
                    file_path = os.path.join(STORE_DIR, request.filename)
                    with open(file_path, "wb") as f:
                        f.write(request.buffer)
                else:
                    with open(file_path, "ab") as f:
                        f.write(request.buffer)

            return chord_pb2.UploadFileResponse(message=f"File uploaded successfully at node : {self.node.ip}:{self.node.port}")
        except Exception as e:
            logger.error(
                f"[Node ID: {self.node.node_id}] Error uploading file: {e}")
            return chord_pb2.UploadFileResponse(message=f"Error uploading file: {e}")

    def Gossip(self, request, context):
        """
        Facilitates gossip communication between nodes to propagate information such as data changes or
        node status updates across the network. This method helps in maintaining data consistency and network awareness.

        Args:
            request (chord_pb2.GossipRequest): The request object containing the gossip message and a unique message ID.
            context (grpc.ServicerContext): Provides RPC-specific methods and properties, aiding in the management
                                            of the RPC and ensuring appropriate response behavior.

        Returns:
            chord_pb2.Empty: Returns an empty protobuf object indicating successful processing and propagation of the gossip message.
        """

        try:
            message = request.message
            message_id = request.message_id

            if message_id in self.node.received_gossip_message_ids:
                return chord_pb2.Empty()

            self.node.received_gossip_message_ids.add(message_id)
            self.node.received_gossip_messages.append(message)

            print(
                f"Peer received gossip message: {message} with message ID: {message_id}", flush=True)
            logger.info(
                f"[Node ID: {self.node.node_id}] Peer received gossip message: {message} with message ID: {message_id}")

            self.node.perform_gossip(message_id, message)
            return chord_pb2.Empty()

        except Exception as e:
            logger.error(
                f"[Node ID: {self.node.node_id}] Error while receiving gossip message.")


def start_server():
    try:
        args = get_args()
        m = M
        node_ip_address = args.ip
        node_port = args.port
        node_id = sha1_hash(f"{node_ip_address}:{node_port}", m)
        bootstrap_ip = args.bootstrap_ip
        bootstrap_port = args.bootstrap_port
        interactive_mode = args.interactive
        chord_node = Node(node_id, node_ip_address, node_port, m)
        bootstrap_node = None

        if bootstrap_ip:
            bootstrap_node = Node(sha1_hash(
                f"{bootstrap_ip}:{bootstrap_port}", m), bootstrap_ip, bootstrap_port, m)

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        chord_pb2_grpc.add_ChordServiceServicer_to_server(
            ChordNodeServicer(chord_node), server)

        # Insecure port configuration
        server.add_insecure_port(f"[::]:{node_port}")

        # Secure port configuration
        # ssl_credentials = load_ssl_credentials()
        # server.add_secure_port(f"[::]:{node_port}", ssl_credentials)

        server.start()
        start_time = time.time()
        chord_node.join_chord_ring(bootstrap_node)
        end_time = time.time()

        print(
            f"Peer started running at {node_ip_address}:{node_port} with ID: {node_id}", flush=True)
        
        print("Time taken to join the ring:", end_time - start_time, flush=True)
        
        logger.info(
            f"[Node ID: {node_id}] Peer started running at {node_ip_address}:{node_port} with ID: {node_id}")

        def run_input_loop():
            while True:
                print(menu_options())
                inp = input("\nSelect an option: ")
                if inp == "1":
                    chord_node.show_finger_table()
                elif inp == "2":
                    print(f"Successor: {chord_node.successor}")
                    print("Successor List:", chord_node.successor_list)
                elif inp == "3":
                    print(f"Predecessor: {chord_node.predecessor}")
                elif inp == "4":
                    chord_node.show_store()
                    print("Received Gossip Messages:")
                    print(chord_node.received_gossip_messages)
                elif inp == "5":
                    file_path = input("Enter the filename to upload: ")
                    if os.path.exists(file_path):
                        upload_response = chord_node.upload_file(file_path)
                        print(upload_response.message)
                    else:
                        print("File not found.")
                elif inp == "6":
                    key = input("Enter the filename to download: ")
                    get_result = chord_node.get(key)
                    file_location_port = get_result.port
                    file_location_ip = get_result.ip
                    print(get_result)
                    if not file_location_ip or not file_location_port:
                        print("File not found in chord ring!")
                    else:
                        print(
                            f"File Found At Node: {file_location_ip}:{file_location_port}")
                        download_file(key, file_location_ip,
                                      file_location_port)
                elif inp == "7":
                    message = input("Enter the message to gossip: ")
                    chord_node.perform_gossip(uuid.uuid4().hex, message)
                elif inp == "8":
                    chord_node.leave()
                    break
                elif inp == "9":
                    print("Shutting down the server.")
                    break
                else:
                    print("Invalid option. Please try again.")
            server.stop(0)

        stabilization_thread = threading.Thread(
            target=run_stabilization, args=(chord_node,))
        stabilization_thread.daemon = True
        stabilization_thread.start()

        if interactive_mode:
            thread = threading.Thread(target=run_input_loop)
            thread.start()
            thread.join()

        # Wait for the server termination and input thread to finish
        server.wait_for_termination()

    except Exception as e:
        print("Encountered Some Error While Starting The Node", e)


if __name__ == "__main__":
    start_server()
