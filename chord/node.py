import grpc
from prettytable import PrettyTable, ALL

from proto import chord_pb2
from proto import chord_pb2_grpc
from utils import create_stub, is_in_between, sha1_hash, generate_requests
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
            try:
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
                        print("Found successor node {}.".format(self.successor))
                    except Exception as e:
                        print(
                            "Error connecting to the predecessor node: {}".format(e))
                        return

                self_stub, self_channel = create_stub(self.ip, self.port)
                with self_channel:
                    set_successor_request = chord_pb2.NodeInfo(
                        id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port
                    )
                    self_stub.SetSuccessor(set_successor_request, timeout=5)

                self.initialize_finger_table(bootstrap_node)
                print("Finger table initialized successfully.")
                print("Starting to update others.")
                self.update_other_nodes()
                print("Successfully updated others about this join.")

                print(
                    "Updating this node's successor's predecessor pointer to this node.")

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

                print("Successfully updated the successor's predecessor pointer.")

                self_stub, self_channel = create_stub(
                    self.ip, self.port)

                with self_channel:
                    get_predecessor_request = chord_pb2.Empty()
                    get_predecessor_response = self_stub.GetPredecessor(
                        get_predecessor_request, timeout=5)
                    self.predecessor = Node(
                        get_predecessor_response.id, get_predecessor_response.ip, get_predecessor_response.port, self.m)

                print(
                    "Updating this node's predecessor's successor pointer to this node.")
                predecessor_stub, predecessor_channel = create_stub(
                    self.predecessor.ip, self.predecessor.port)

                with predecessor_channel:
                    set_successor_request = chord_pb2.NodeInfo(
                        id=self.node_id, ip=self.ip, port=self.port)
                    predecessor_stub.SetSuccessor(
                        set_successor_request, timeout=5)

                print("Successfully updated the predecessor's successor pointer.")

                print("Initializing hash table to get this node's keys.")
                self.initialize_store()
                print("Successfully initialized hash table.")

                print("Starting replication to successors.")
                # TODO - Implement replication bit
                self.replicate_keys_to_successor()
                print("Replication successful.")

            except Exception as e:
                print("Error joining the chord ring through bootstrap node: {}".format(
                    bootstrap_node.port))
                print(e)

    def i_start(self, node_id, i) -> int:

        start = (node_id + (2 ** (i - 1))) % (2 ** self.m)
        return int(start)

    def initialize_finger_table(self, bootstrap_node):

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
            if is_in_between(self.i_start(self.node_id, i+2), self.node_id, self.finger_table[i].node_id, 'l'):
                self.finger_table[i+1] = self.finger_table[i]

            else:
                bootstra_stub, bootstrap_channel = create_stub(
                    bootstrap_node.ip, bootstrap_node.port)
                with bootstrap_channel:
                    find_successor_request = chord_pb2.FindSuccessorRequest(
                        id=self.i_start(self.node_id, i+2)
                    )
                    find_successor_response = bootstra_stub.FindSuccessor(
                        find_successor_request, timeout=5)
                    self.finger_table[i+1] = Node(find_successor_response.id,
                                                  find_successor_response.ip, find_successor_response.port, self.m)

    def go_back_n(self, node_id, i) -> int:

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
            # print("CAME IN HERE", i)
            update_id = self.go_back_n(self.node_id, 2**(i))
            # update_id = self.node_id - 2**i
            # if update_id < 0:
            #     update_id = self.node_id + (2**self.m - 2**i)
            # print("Update id: ", update_id)
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
                pred_stub.UpdateFingerTable(
                    update_finger_table_request, timeout=5)

        print("Updated other nodes successfully.")

    def fix_fingers(self):
        """
        Fix the finger table of the node
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
            print("Error fixing finger table: {}".format(e))
            return
        # print("Fingers fixed successfully.")

    def stabilize(self):

        try:
            stub, channel = create_stub(self.ip, self.port)
            # print("Starting stabilization")
            with channel:
                set_successor_request = chord_pb2.NodeInfo(
                    id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port)
                stub.SetSuccessor(set_successor_request, timeout=5)
        except Exception:
            return
        # print("finished stabilization")

    def initialize_store(self):

        successor_stub, successor_channel = create_stub(
            self.successor.ip, self.successor.port)
        with successor_channel:
            get_transfer_data_request = chord_pb2.GetTransferDataRequest(
                id=self.node_id)
            get_transfer_data_response = successor_stub.GetTransferData(
                get_transfer_data_request, timeout=5)

            data = get_transfer_data_response.data
        self.store = ast.literal_eval(data)

    def set(self, key, filename="not_provided.txt"):
        # print(f"Node.py set() called for key --> {key}")
        hashed_key = sha1_hash(key, self.m)
        # print(f"Node.py set() the hashed key value --> {hashed_key}")
        stub, channel = create_stub(self.ip, self.port)
        with channel:
            find_successor_request = chord_pb2.FindSuccessorRequest(
                id=hashed_key)
            find_successor_response = stub.FindSuccessor(
                find_successor_request, timeout=5)
            # print(f"Node.py set() called by {self.node_id} ,possible node where the value would be set is {stub.FindSuccessor(find_successor_request ,timeout=5)}")
            # todo added by suryakangeyan -->   call set_key here to set the value to the particular  node

        successor_stub, successor_channel = create_stub(
            find_successor_response.ip, find_successor_response.port)

        with successor_channel:
            set_key_request = chord_pb2.SetKeyRequest(
                key=hashed_key, filename = filename
            )
            set_key_response = successor_stub.SetKey(
                set_key_request, timeout=5)

        return set_key_response

    def get(self, key):
        # print(f"Node.py get() called for key --> {key}")
        hashed_key = sha1_hash(key, self.m)
        # print(f"Node.py get() the hashed key value --> {hashed_key}")
        stub, channel = create_stub(self.ip, self.port)
        with channel:
            find_successor_request = chord_pb2.FindSuccessorRequest(
                id=hashed_key)
            # print(
            # f"Node.py set() called by {self.node_id} ,possible node where the value would be set is {stub.FindSuccessor(find_successor_request, timeout=5)}")
            # todo added by suryakangeyan -->   call get_key here to set the value to the particular  node
            find_successor_response = stub.FindSuccessor(
                find_successor_request, timeout=5)

        successor_stub, successor_channel = create_stub(
            find_successor_response.ip, find_successor_response.port)

        with successor_channel:
            get_key_request = chord_pb2.GetKeyRequest(
                key=hashed_key
            )
            get_key_response = successor_stub.GetKey(
                get_key_request, timeout=5)

        return get_key_response

    def replicate_to_successor(self, store=None):
        if not store:
            build_store = {}
            for key in self.store:
                if self.store[key][0]:
                    build_store[key][0] = False

            # stub,channel = create_stub(self.ip,self.port)
            # with channel :  todo need to check if this should be an RPC or just a normal method call
            # self.successor.

    def receive_keys_before_leave(self, store):

        for key in store:
            self.store[key][0] = store[key][0]

    def replicate_keys_to_successor(self, store=None):

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
            else:
                successor_stub, successor_channel = create_stub(
                    successor.ip, successor.port)
                with successor_channel:
                    receive_keys_before_leave_request = chord_pb2.ReceiveKeysBeforeLeaveRequest(
                        store=str(store)
                    )
                    successor_stub.ReceiveKeysBeforeLeave(
                        receive_keys_before_leave_request, timeout=5)

    def replicate_single_key_to_successor(self, key):
        store = {key: [False, self.store[key][1]]}
        self.replicate_keys_to_successor(store)

    def transfer_before_leave(self):

        successor_stub, successor_channel = create_stub(
            self.successor.ip, self.successor.port)
        with successor_channel:
            receive_keys_before_leave_request = chord_pb2.ReceiveKeysBeforeLeaveRequest(
                store=str(self.store)
            )
            successor_stub.ReceiveKeysBeforeLeave(
                receive_keys_before_leave_request, timeout=5)

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
            nodeinfo = chord_pb2.NodeInfo(
                id=self.successor.node_id, ip=self.successor.ip, port=self.successor.port)
            update_finger_table_request = chord_pb2.UpdateFingerTableRequest(
                node=nodeinfo, i=0, for_leave=True)
            predecessor_stub.UpdateFingerTable(
                update_finger_table_request, timeout=5)
        print("Transferring keys to responsible node.")
        self.transfer_before_leave()
        print("Node {} left the system successfully.".format(self.node_id))

    def upload_file(self, file_path):
        hashed_key = sha1_hash(file_path, self.m)
        stub, channel = create_stub(self.ip, self.port)
        with channel:
            set_key_request = chord_pb2.SetKeyRequest(key=hashed_key, filename=file_path)
            set_key_response = stub.SetKey(set_key_request, timeout=5)
            target_node_port = set_key_response.port
            target_node_ip = set_key_response.ip

        target_node_stub, target_node_channel = create_stub(
            target_node_ip, target_node_port)
        with target_node_channel:
            upload_file_response = target_node_stub.UploadFile(
                generate_requests(file_path), timeout=5)

        return upload_file_response
    

    def show_store(self):
        table = [["No.", "File Name"]]
        for i, key in enumerate(self.store, start=1):
            table.append([i, self.store[key][1]])
        
        tab = PrettyTable(table[0])
        tab.add_rows(table[1:])
        tab.hrules = ALL
        print(tab)

    def show_finger_table(self):
        table = [["i", "Start", "Successor"]]
        for i in range(self.m):
            table.append([i, self.i_start(self.node_id, i), self.finger_table[i].node_id])
        
        tab = PrettyTable(table[0])
        tab.add_rows(table[1:])
        tab.hrules = ALL
        print(tab)
