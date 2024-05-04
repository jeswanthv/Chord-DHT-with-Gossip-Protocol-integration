import grpc
from proto import chord_pb2
from proto import chord_pb2_grpc
import threading
from concurrent import futures

from utils import sha1_hash, get_args
from chord.node import Node


class ChordNodeServicer(chord_pb2_grpc.ChordServiceServicer):

    def __init__(self, node: Node):
        self.node = node

    def GetSuccessor(self, request, context):
        response = chord_pb2.NodeInfo()
        response.node_id = self.node.successor.node_id
        response.ip_address = self.node.successor.ip
        response.port = self.node.successor.port

        return response

    def GetPredecessor(self, request, context):
        response = chord_pb2.NodeInfo()
        response.node_id = self.node.predecessor.node_id
        response.ip_address = self.node.predecessor.ip
        response.port = self.node.predecessor.port

        return response

    def FindPredecessor(self, request, context):

        print("Finding predecessor rpc called with port", self.node.port)

        # if first node in the ring
        if self.node.predecessor is None or self.node.successor.node_id == self.node.node_id:
            print("Returning self as predecessor")
            response = chord_pb2.NodeInfo()
            response.node_id = self.node.node_id
            response.ip_address = self.node.ip
            response.port = self.node.port
            return response

        else:
            id_to_find = request.id
            current_node = self.node

            # might need to fix this for wrap around case
            while not (current_node.node_id < id_to_find <= current_node.successor.node_id):
                current_node = current_node.closest_preceding_finger(id_to_find)

            response = chord_pb2.NodeInfo()
            response.node_id = current_node.node_id
            response.ip_address = current_node.ip
            response.port = current_node.port
            return response

    def FindSuccessor(self, request, context):
        """
        FindSuccessor RPC call to find the successor node of a given node_id.
        LINEAR SEARCH IMPLEMENTATION
        """
        print("Finding successor rpc called with port", self.node.port)
        id_to_find = request.node_id
        my_id = self.node.node_id
        my_successor_id = self.node.successor.node_id

        # Check if the requested ID is in the range (my_id, my_successor_id]
        if (my_id < id_to_find <= my_successor_id) or (my_id > my_successor_id and (id_to_find > my_id or id_to_find <= my_successor_id)):
            # The current node's successor is the successor of the requested node_id
            response = chord_pb2.NodeInfo()
            response.node_id = self.node.successor.node_id
            response.ip_address = self.node.successor.ip
            response.port = self.node.successor.port
            return response
        else:
            # Need to ask the successor to find the successor
            channel = grpc.insecure_channel(
                f'{self.node.successor.ip}:{self.node.successor.port}')
            stub = chord_pb2_grpc.ChordServiceStub(channel)
            successor_request = chord_pb2.FindSuccessorRequest(id=id_to_find)
            return stub.FindSuccessor(successor_request)

    def InitFingerTable(self, request, context):
        """
        Initialize the finger table of the node
        """

        # Initialize the first finger entry
        client = grpc.insecure_channel(f'{self.node.ip}:{self.node.port}')
        stub = chord_pb2_grpc.ChordServiceStub(client)
        request = chord_pb2.FindSuccessorRequest(
            node_id=(self.node.node_id + 2**0) % (2**self.node.m))
        response = stub.FindSuccessor(request)
        self.node.finger_table[0] = Node(
            response.node_id, response.ip_address, response.port, self.node.m)
        self.node.successor = self.node.finger_table[0]

        # Set the predecessor
        self.node.predecessor = self.node.successor.predecessor

        # Update the predecessor's successor
        self.node.successor.predecessor = self.node

        # Update the previous node's successor
        self.node.finger_table[0].predecessor.successor = self.node

        # Update the rest of the finger table
        for i in range(1, self.node.m):
            if self.node.finger_table[i-1] and self.node.finger_table[i-1].node_id < (self.node.node_id + 2**i) % (2**self.node.m):
                self.node.finger_table[i] = self.node.finger_table[i-1]
            else:
                self.node.finger_table[i] = self.node.find_successor(
                    (self.node.node_id + 2**i) % (2**self.node.m))

        return chord_pb2.Empty()


def start_server():
    try:
        args = get_args()
        m = 10
        node_ip_address = args.ip
        node_port = args.port
        node_id = sha1_hash(f"{node_ip_address}:{node_port}", m)
        bootstrap_ip = args.bootstrap_ip
        bootstrap_port = args.bootstrap_port

        chord_node = Node(node_id, node_ip_address, node_port, m)
        bootstrap_node = None

        if bootstrap_ip:
            bootstrap_node = Node(sha1_hash(
                f"{bootstrap_ip}:{bootstrap_port}", m), bootstrap_ip, bootstrap_port, m)

        chord_node.join_chord_ring(bootstrap_node)

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        chord_pb2_grpc.add_ChordServiceServicer_to_server(
            ChordNodeServicer(chord_node), server)
        server.add_insecure_port(f"[::]:{node_port}")
        server.start()
        print(f"Server started at {node_ip_address}:{node_port}")

        def run_input_loop():
            while True:
                inp = input(
                    "Select an option:\n1. Print Finger Table\n2. Print Successor\n3. Print Predecessor\n4. Quit\n")
                if inp == "1":
                    print(chord_node.finger_table)
                elif inp == "2":
                    print(chord_node.successor)
                elif inp == "3":
                    print(chord_node.predecessor)
                elif inp == "4":
                    print("Shutting down the server.")
                    server.stop(0)
                    break
                else:
                    print("Invalid option. Please try again.")

        # Start the input loop in a separate thread
        thread = threading.Thread(target=run_input_loop)
        thread.start()

        # Wait for the server termination and input thread to finish
        server.wait_for_termination()
        thread.join()

    except Exception as e:
        print("error occurred", e)


if __name__ == "__main__":
    start_server()
