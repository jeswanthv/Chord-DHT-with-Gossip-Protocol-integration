import grpc
from proto import chord_pb2
from proto import chord_pb2_grpc
from utils import create_stub


class Node:
    def __init__(self, node_id, ip, port, m):
        self.ip = ip
        self.port = port
        self.node_id = node_id
        self.m = m
        self.finger_table = {}
        self.predecessor = None
        self.successor = None
        self.finger_table = {i: None for i in range(m)}
        self.successor_list = []

    def __str__(self):
        return f"Node {self.node_id} at {self.ip}:{self.port}"

    def join_chord_ring(self, bootstrap_node):
        """
        Join an existing node in the chord ring
        """

        if not bootstrap_node:
            # Create a new ring
            self.successor = self
            self.predecessor = self
            for i in range(self.m):
                self.finger_table[i] = self
        else:
            # Join an existing ring
            # client = grpc.insecure_channel(
            #     f'{bootstrap_node.ip}:{bootstrap_node.port}')
            # stub = chord_pb2_grpc.ChordServiceStub(client)
            bootstrap_stub, bootstrap_channel = create_stub(
                bootstrap_node.ip, bootstrap_node.port)

            with bootstrap_channel:
                find_predecessor_request = chord_pb2.NodeInfo(
                    ip=self.ip, id=self.node_id, port=self.port)
                find_predecessor_response = bootstrap_stub.FindPredecessor(
                    find_predecessor_request)

                self.predecessor = Node(find_predecessor_response.node_id,
                                        find_predecessor_response.ip_address,
                                        find_predecessor_response.port, self.m)

            predecessor_stub, predecessor_channel = create_stub(
                self.predecessor.ip, self.predecessor.port)

            with predecessor_channel:
                get_successor_request = chord_pb2.Empty()
                get_successor_response = predecessor_stub.GetSuccessor(
                    get_successor_request)
                self.successor = Node(get_successor_response.node_id,
                                      get_successor_response.ip_address,
                                      get_successor_response.port, self.m)

    def initialize_finger_table(self):
        """
        Initialize the finger table of the node
        """

        successor = self.successor

        stub, channel = create_stub(
            successor.ip, successor.port)

        with channel:
            get_predecessor_request = chord_pb2.Empty()
            get_predecessor_response = stub.GetPredecessor(
                get_predecessor_request)
            self.predecessor = Node(get_predecessor_response.node_id,
                                    get_predecessor_response.ip_address,
                                    get_predecessor_response.port, self.m)

        print("Initializing the first finger to successor node {}.".format(
            str(self.successor)))
        self.finger_table[0] = self.successor

        self.successor = successor

        for i in range(1, self.m):

            finger_start = (self.node_id + 2**i) % (2**self.m)
            my_id = self.node_id
            my_successor_id = self.successor.node_id
            id_to_find = finger_start
            if (my_id < id_to_find <= my_successor_id) or \
                    (my_id > my_successor_id and
                        (id_to_find > my_id or id_to_find <= my_successor_id)):
                self.finger_table[i] = self.successor
            else:
                stub, channel = create_stub(
                    self.ip, self.port)

                with channel:
                    find_successor_request = chord_pb2.FindSuccessorRequest(
                        node_id=id_to_find)
                    find_successor_response = stub.FindSuccessor(
                        find_successor_request)
                    
                    self.finger_table[i] = Node(find_successor_response.node_id,
                                                find_successor_response.ip_address,
                                                find_successor_response.port, self.m)

        