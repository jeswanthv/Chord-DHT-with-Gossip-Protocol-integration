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

        print("Starting join ....")

        if not bootstrap_node:
            print("No bootstrap server provided. Starting a new chord ring.")
            self.successor = self
            self.predecessor = self
            for i in range(self.m):
                self.finger_table[i] = self
            print("Finger table initialized to self.")
        else:
            print("Joining an existing chord ring with bootstrap server {}.".format(bootstrap_node))
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
                print("Found predecessor node {}.".format(self.predecessor))

            predecessor_stub, predecessor_channel = create_stub(
                self.predecessor.ip, self.predecessor.port)

            with predecessor_channel:
                get_successor_request = chord_pb2.Empty()
                get_successor_response = predecessor_stub.GetSuccessor(
                    get_successor_request)
                self.successor = Node(get_successor_response.node_id,
                                      get_successor_response.ip_address,
                                      get_successor_response.port, self.m)
                print("Found successor node {}.".format(self.successor))

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
                    
    def closest_preceding_finger(self, id):
        """
        Find the closest preceding finger of a node
        """

        # for i in range(self.m - 1, -1, -1):
        #     if self.finger_table[i] is not None:
        #         if self.node_id < self.finger_table[i].node_id < id or \
        #                 (self.node_id > id and
        #                     (self.finger_table[i].node_id > self.node_id or
        #                         self.finger_table[i].node_id < id)):
        #             return self.finger_table[i]

        # return self

        # might need correction for the wrap-around case
        for i in range(self.m - 1, -1, -1):
            finger_id = self.finger_table[i].node_id if self.finger_table[i] else None
            if finger_id:
                # Check if finger is between current node_id and id in a clockwise manner
                if self.node_id < id:
                    if self.node_id < finger_id < id:
                        return self.finger_table[i]
                else:  # This handles the wrap-around case
                    if finger_id > self.node_id or finger_id < id:
                        return self.finger_table[i]
    # If no valid finger is found, return self to signify this node handles the request
        return self
    
    

        