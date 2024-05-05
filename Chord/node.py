import grpc
from proto import chord_pb2
from proto import chord_pb2_grpc
from utils import create_stub, is_in_between
import random


class Node:
    def __init__(self, node_id: int, ip: str, port: int, m):
        self.ip = str(ip)
        self.port = int(port)
        self.node_id = int(node_id)
        self.m = m
        self.finger_table = {}
        self.predecessor = None
        self.successor = None
        self.finger_table = {i: None for i in range(m)}
        self.successor_list = [self for _ in range(3)]
        self.store = {}

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
            print("Joining an existing chord ring with bootstrap server {}.".format(
                bootstrap_node))
            bootstrap_stub, bootstrap_channel = create_stub(
                bootstrap_node.ip, bootstrap_node.port)

            with bootstrap_channel:

                try:
                    find_predecessor_request = chord_pb2.FindPredecessorRequest(
                        id=self.node_id)
                    find_predecessor_response = bootstrap_stub.FindPredecessor(
                        find_predecessor_request, timeout=5)
                except Exception as e:
                    print("Error connecting to the bootstrap node: {}".format(e))
                    return

                self.predecessor = Node(find_predecessor_response.id,
                                        find_predecessor_response.ip,
                                        find_predecessor_response.port, self.m)
                print("Found predecessor node {}.".format(self.predecessor))

            predecessor_stub, predecessor_channel = create_stub(
                self.predecessor.ip, self.predecessor.port)

            with predecessor_channel:
                try:
                    get_successor_request = chord_pb2.Empty()
                    get_successor_response = predecessor_stub.GetSuccessor(
                        get_successor_request)
                    self.successor = Node(get_successor_response.id,
                                          get_successor_response.ip,
                                          get_successor_response.port, self.m)
                    print("Found successor node {}.".format(self.successor))
                except Exception as e:
                    print("Error connecting to the predecessor node: {}".format(e))
                    return

            self.initialize_finger_table(bootstrap_node)
            print("Finger table initialized successfully.")
            print("Starting to update others.")
            self.update_other_nodes()
            print("Successfully updated others about this join.")

            print("Updating this node's successor's predecessor pointer to this node.")
            successor_stub, successor_channel = create_stub(
                self.successor.ip, self.successor.port)

            with successor_channel:
                set_predecessor_request = chord_pb2.NodeInfo(
                    id=self.node_id, ip=self.ip, port=self.port)
                successor_stub.SetPredecessor(set_predecessor_request)

            print("Successfully updated the successor's predecessor pointer.")

            print("Updating this node's predecessor's successor pointer to this node.")
            predecessor_stub, predecessor_channel = create_stub(
                self.predecessor.ip, self.predecessor.port)
            
            with predecessor_channel:
                set_successor_request = chord_pb2.NodeInfo(
                    id=self.node_id, ip=self.ip, port=self.port)
                predecessor_stub.SetSuccessor(set_successor_request)

            print("Successfully updated the predecessor's successor pointer.")


    def initialize_finger_table(self, bootstrap_node):
        """
        Initialize the finger table of the node
        """

        successor = self.successor

        stub, channel = create_stub(
            bootstrap_node.ip, bootstrap_node.port)

        with channel:
            get_predecessor_request = chord_pb2.Empty()
            get_predecessor_response = stub.GetPredecessor(
                get_predecessor_request)
            self.predecessor = Node(get_predecessor_response.id,
                                    get_predecessor_response.ip,
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

                    self.finger_table[i] = Node(find_successor_response.id,
                                                find_successor_response.ip,
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

    def update_other_nodes(self):
        """
        Update other nodes in the ring about the new node
        """

        for i in range(self.m):
            # go_back_n part
            update_id = self.node_id - 2**i
            if update_id < 0:
                update_id = self.node_id + (2**self.m - 2**i)

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
                pred_stub.UpdateFingerTable(update_finger_table_request)

        print("Updated other nodes successfully.")

    def fix_fingers(self):
        """
        Fix the finger table of the node
        """

        i = random.randint(0, self.m - 1)
        finger_start = (self.node_id + 2**i) % (2**self.m)
        stub, channel = create_stub(self.ip, self.port)
        with channel:
            find_successor_request = chord_pb2.FindSuccessorRequest(
                id=finger_start)
            find_successor_response = stub.FindSuccessor(
                find_successor_request)
            self.finger_table[i] = Node(find_successor_response.id,
                                        find_successor_response.ip,
                                        find_successor_response.port, self.m)

        print("Fingers fixed successfully.")

    def leave(self):
        """
        Graceful leaving of the node form the network
        """

        predecessor_stub,predecessor_stub_channel =create_stub(self.predecessor.ip,self.predecessor.port)
        successor_stub,successor_stub_channel  = create_stub(self.successor.ip, self.successor.port)

        with successor_stub_channel,predecessor_stub_channel :
            set_successor_request = chord_pb2.NodeInfo(
                id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port)
            predecessor_stub.SetSuccessor(set_successor_request)

        set_predecessor_request = chord_pb2.NodeInfo(
            id=self.predecessor.node_id, ip=self.predecessor.ip, port=self.predecessor.port)
        successor_stub.SetPredecessor(set_predecessor_request)

        update_finger_table_request = chord_pb2.UpdateFingerTableRequest(
            node=chord_pb2.NodeInfo(
                id=self.successor.node_id,
                ip=self.successor.ip,
                port=self.successor.port),
            i=0,
            for_leave=True
        )
        predecessor_stub.UpdateFingerTable(update_finger_table_request)

        print("Node.py - Node gracefully leaving the network, leave success")


