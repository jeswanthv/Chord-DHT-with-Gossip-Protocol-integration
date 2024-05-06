import grpc
from proto import chord_pb2
from proto import chord_pb2_grpc
from utils import create_stub, is_in_between
import random
import ast

class Node:
    def __init__(self, node_id: int, ip: str, port: int, m):
        self.ip = str(ip)
        self.port = int(port)
        self.node_id = int(node_id)
        self.m = m
        self.finger_table = {}
        self.predecessor = None
        self.successor = None
        self.finger_table = {i: self for i in range(m)}
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
            
            self_stub, self_channel = create_stub(self.ip, self.port)
            print("HEEERE")
            with self_channel:
                set_predecessor_request = chord_pb2.NodeInfo(
                    id= self.predecessor.node_id, ip=self.predecessor.ip, port=self.predecessor.port
                )
                self_stub.SetPredecessor(set_predecessor_request, timeout=5)



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
                    print("Found successor node {}.".format(self.successor))
                except Exception as e:
                    print("Error connecting to the predecessor node: {}".format(e))
                    return
            
            self_stub, self_channel = create_stub(self.ip, self.port)
            print("HEEERE2")
            with self_channel:
                set_successor_request = chord_pb2.NodeInfo(
                    id= self.successor.node_id, ip=self.successor.ip, port=self.successor.port
                )
                self_stub.SetSuccessor(set_successor_request, timeout=5)

            self.initialize_finger_table(bootstrap_node)
            print("Finger table initialized successfully.")
            print("Starting to update others.")
            self.update_other_nodes()
            print("Successfully updated others about this join.")

            print("Updating this node's successor's predecessor pointer to this node.")

            self_stub, self_channel = create_stub(
                self.ip, self.port)
            
            with self_channel:
                get_successor_request = chord_pb2.Empty()
                get_successor_response = self_stub.GetSuccessor(get_successor_request, timeout=5)
                self.successor = Node(get_successor_response.id, get_successor_response.ip, get_successor_response.port, self.m)

            successor_stub, successor_channel = create_stub(
                self.successor.ip, self.successor.port)

            with successor_channel:
                set_predecessor_request = chord_pb2.NodeInfo(
                    id=self.node_id, ip=self.ip, port=self.port)
                successor_stub.SetPredecessor(set_predecessor_request, timeout=5)

            print("Successfully updated the successor's predecessor pointer.")

            self_stub, self_channel = create_stub(
                self.ip, self.port)
            
            with self_channel:
                get_predecessor_request = chord_pb2.Empty()
                get_predecessor_response = self_stub.GetPredecessor(get_predecessor_request, timeout=5)
                self.predecessor = Node(get_predecessor_response.id, get_predecessor_response.ip, get_predecessor_response.port, self.m)

            print("Updating this node's predecessor's successor pointer to this node.")
            predecessor_stub, predecessor_channel = create_stub(
                self.predecessor.ip, self.predecessor.port)
            
            with predecessor_channel:
                set_successor_request = chord_pb2.NodeInfo(
                    id=self.node_id, ip=self.ip, port=self.port)
                predecessor_stub.SetSuccessor(set_successor_request, timeout=5)

            print("Successfully updated the predecessor's successor pointer.")

            print("Initializing hash table to get this node's keys.")
            self.initialize_store()
            print("Successfully initialized hash table.")

            print("Starting replication to successors.")

    def i_start(self, node_id, i) -> int:
        """
        Author: Adarsh Trivedi
        Helper function.
        :param node_id: Current node id.
        :param i: node_id + 2^i
        :return: node_id + 2^i
        """
        start = (node_id + (2 ** (i - 1))) % (2 ** self.m)
        return start
    
    def initialize_finger_table(self, bootstrap_node):

        successor = self.successor

        successor_stub, successor_channel = create_stub(
            successor.ip, successor.port)
        
        with successor_channel:
            get_predecessor_request = chord_pb2.Empty()
            get_predecessor_response = successor_stub.GetPredecessor(get_predecessor_request, timeout=5)
            self.predecessor = Node(get_predecessor_response.id, get_predecessor_response.ip, get_predecessor_response.port, self.m)

        self.finger_table[0] = self.successor

        for i in range(self.m-1):
            # finger_start = (self.node_id + 2**i) % (2**self.m)
            if is_in_between(self.i_start(self.node_id, i+2),self.node_id, self.finger_table[i].node_id,'l'):
                self.finger_table[i+1] = self.finger_table[i]

            else:
                bootstra_stub, bootstrap_channel = create_stub(bootstrap_node.ip, bootstrap_node.port)
                with bootstrap_channel:
                    find_successor_request = chord_pb2.FindSuccessorRequest(
                        id=self.i_start(self.node_id, i+2)
                    )
                    find_successor_response = bootstra_stub.FindSuccessor(find_successor_request, timeout=5)
                    self.finger_table[i+1] = Node(find_successor_response.id, find_successor_response.ip, find_successor_response.port, self.m)


    
    def go_back_n(self, node_id, i) -> int:
        """
        Author: Adarsh Trivedi
        Helper functions.
        :param node_id: Current node_id
        :param i: How many steps to move back.
        :return: id after moving specified steps back.
        """
        diff = node_id - i

        if diff >= 0:
            return diff
        else:
            return node_id + (2 ** self.m - i)
        
    def update_other_nodes(self):
        """
        Update other nodes in the ring about the new node
        """
        for i in range(self.m):
            # go_back_n part
            print("CAME IN HERE", i)
            update_id = self.go_back_n(self.node_id, 2**(i))
            # update_id = self.node_id - 2**i
            # if update_id < 0:
            #     update_id = self.node_id + (2**self.m - 2**i)
            print("Update id: ", update_id)
            # exit(0)
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
                print("BBBB")
                pred_stub.UpdateFingerTable(update_finger_table_request, timeout=5)
                print("CCCC")

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
                find_successor_request, timeout=5)
            self.finger_table[i] = Node(find_successor_response.id,
                                        find_successor_response.ip,
                                        find_successor_response.port, self.m)

        print("Fingers fixed successfully.")

    def stabilize(self):

        stub, channel = create_stub(self.ip, self.port)
        print("Starting stabilization")
        with channel:
            set_successor_request = chord_pb2.NodeInfo(
                id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port)
            stub.SetSuccessor(set_successor_request, timeout=5)

        print("finished stabilization")


    def leave(self):

        print("Starting to leave the system.")
        print("Setting predecessor's [{}] successor to this node's successor [{}].".
                    format(self.predecessor, self.successor))
        predecessor_stub, predecessor_channel = create_stub(
            self.predecessor.ip, self.predecessor.port)
        with predecessor_channel:
            set_successor_request = chord_pb2.NodeInfo(
                id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port)
            predecessor_stub.SetSuccessor(set_successor_request, timeout=5)
        print("Setting successor's [{}] predecessor to this node's predecessor [{}].".
                    format(self.successor, self.predecessor))
        successor_stub, successor_channel = create_stub(
            self.successor.ip, self.successor.port)
        with successor_channel:
            set_predecessor_request = chord_pb2.NodeInfo(
                id=self.predecessor.node_id, ip=self.predecessor.ip, port=self.predecessor.port)
            successor_stub.SetPredecessor(set_predecessor_request, timeout=5)
        print("Updating 1st finger (successor) to this node's successor.")
        predecessor_stub, predecessor_channel = create_stub(
            self.predecessor.ip, self.predecessor.port)
        with predecessor_channel:
            print("HEREEA calling on", self.predecessor.node_id, self.predecessor.port)
            print("params", self.successor.node_id, self.successor.port, self.successor.ip)

            nodeinfo = chord_pb2.NodeInfo(id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port)
            update_finger_table_request = chord_pb2.UpdateFingerTableRequest(
                node=nodeinfo, i=0, for_leave=True)
            predecessor_stub.UpdateFingerTable(update_finger_table_request, timeout=5)
            print("HEREEA2")
        print("Transferring keys to responsible node.")
        # self.transfer_before_leave()
        print("Node {} left the system successfully.".format(self.node_id))


    def initialize_store(self):

        successor_stub, successor_channel = create_stub(
            self.successor.ip, self.successor.port)
        with successor_channel:
            get_transfer_data_request = chord_pb2.GetTransferDataRequest(
                id=self.node_id)
            get_transfer_data_response = successor_stub.GetTransferData(get_transfer_data_request, timeout=5)
            
            data = get_transfer_data_response.data

            
        self.store = ast.literal_eval(data)
