import grpc
from proto import chord_pb2
from proto import chord_pb2_grpc
import threading
from concurrent import futures
import ast
import os
from utils import sha1_hash, get_args, create_stub, is_in_between, download_file
from chord.node import Node
import time
import uuid


def run_stabilization(node):
    while True:
        try:
            node.stabilize()
            node.fix_fingers()
        except Exception as e:
            print("Error in stabilization loop: ", e)
        time.sleep(2)  # Sleep for 10 seconds or any other suitable interval


class ChordNodeServicer(chord_pb2_grpc.ChordServiceServicer):

    def __init__(self, node: Node):
        self.node = node

    def GetSuccessor(self, request, context):

        response = chord_pb2.NodeInfo()
        response.id = self.node.successor.node_id
        response.ip = self.node.successor.ip
        response.port = self.node.successor.port
        return response

    def SetSuccessor(self, request, context):
        # NEED TO COMPLETE
        id = request.id
        ip = request.ip
        port = request.port
        self.node.successor = Node(id, ip, port, self.node.m)
        self.node.successor_list[0] = self.node.successor

        i = 1
        while i < 3:
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
            except Exception as e:
                print("BOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOM")
                self.node.successor_list[i-1] = self.node.successor_list[i]
                if i == 1:
                    print("Suspect a crash for node [{}].".format(
                        self.node.successor))
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
                        print("Will try again updating the predecessor.", i)

                    self.node.replicate_keys_to_successor()
            i += 1

        return chord_pb2.Empty()

    def GetPredecessor(self, request, context):
        response = chord_pb2.NodeInfo()
        response.id = self.node.predecessor.node_id
        response.ip = self.node.predecessor.ip
        response.port = self.node.predecessor.port

        return response

    def SetPredecessor(self, request, context):
        id = request.id
        ip = request.ip
        port = request.port
        self.node.predecessor = Node(id, ip, port, self.node.m)
        return chord_pb2.Empty()

    def FindPredecessor(self, request, context):

        # if first node in the ring
        if self.node.successor.node_id == self.node.node_id:
            response = chord_pb2.NodeInfo()
            response.id = self.node.node_id
            response.ip = self.node.ip
            response.port = self.node.port
            return response

        else:
            id_to_find = request.id
            current_node = self.node

            while not (is_in_between(id_to_find, current_node.node_id, current_node.successor.node_id, 'right_open')):
                current_node_stub, current_node_channel = create_stub(
                    current_node.ip, current_node.port)

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

    def FindSuccessor(self, request, context):
        """
        FindSuccessor RPC call to find the successor node of a given node_id.
        """
        id_to_find = request.id

        node_stub, node_channel = create_stub(self.node.ip, self.node.port)
        with node_channel:
            find_pred_request = chord_pb2.FindPredecessorRequest(id=id_to_find)
            find_pred_response = node_stub.FindPredecessor(find_pred_request)

        # Ask the predecessor to find the successor
        pred_stub, pred_channel = create_stub(
            find_pred_response.ip, find_pred_response.port)

        with pred_channel:
            get_succ_request = chord_pb2.Empty()
            get_succ_response = pred_stub.GetSuccessor(get_succ_request)

        return get_succ_response

    def FindClosestPrecedingFinger(self, request, context):
        id_to_find = request.id
        response = chord_pb2.NodeInfo()
        # print("self.node.node_id", self.node.node_id)
        # print("CLOSEST PRECEDING FINGER ID", id_to_find)
        # print("FINGER TABLE", self.node.finger_table[9].node_id)
        for i in range(self.node.m-1, -1, -1):
            if is_in_between(self.node.finger_table[i].node_id, self.node.node_id, id_to_find, 'closed'):
                # print("TRUE FOR IDENTIFIER", id_to_find)

                response.id = self.node.finger_table[i].node_id
                response.ip = self.node.finger_table[i].ip
                response.port = self.node.finger_table[i].port
                return response

        response.id = self.node.node_id
        response.ip = self.node.ip
        response.port = self.node.port
        return response

    def UpdateFingerTable(self, request, context):
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

        if source_id != self.node.node_id and is_in_between(source_id, self.node.node_id, self.node.finger_table[i].node_id, 'left_open'):
            self.node.finger_table[i] = Node(
                source_id, source_ip, source_port, self.node.m)
            # Ask the predecessor to update the finger table
            pred = self.node.predecessor

            while True:
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
                    print(f"Error updating finger table: {e}")
                    continue

        return chord_pb2.Empty()

    def GetTransferData(self, request, context):
        node_id = request.id
        transfer_data = {}
        keys_to_be_deleted = []

        for key in self.node.store:
            if is_in_between(key, self.node.node_id, node_id, 'o'):
                # transfer_data[key] = True
                transfer_data[key] = [True, self.node.store[key][1]]
                keys_to_be_deleted.append(key)

        for key in keys_to_be_deleted:
            del self.node.store[key]

        response = chord_pb2.GetTransferDataResponse()
        response.data = str(transfer_data)
        return response

    def SetKey(self, request, context):
        print("SET KEY CALLED", request)
        key = request.key
        self.node.store[key] = [True, request.filename]
        if self.node.node_id != self.node.successor.node_id:
            self.node.replicate_single_key_to_successor(key)

        return chord_pb2.NodeInfo(id=self.node.node_id, ip=self.node.ip, port=self.node.port)

    def GetKey(self, request, context):
        key = request.key
        if key in self.node.store:
            return chord_pb2.NodeInfo(id=self.node.node_id, ip=self.node.ip, port=self.node.port)
        else:
            return chord_pb2.NodeInfo(id=None, ip=None, port=None)

    def ReceiveKeysBeforeLeave(self, request, context):

        store_received = ast.literal_eval(request.store)

        for key in store_received:
            self.node.store[key] = store_received[key]

        return chord_pb2.Empty()

    def DownloadFile(self, request, context):
        file_name = request.filename
        try:
            with open(file_name, 'rb') as f:
                while True:
                    chunk = f.read(1024)
                    if not chunk:
                        break
                    yield chord_pb2.DownloadFileResponse(buffer=chunk)
        except Exception as e:
            print(f"Error downloading file: {e}")
            return

    def UploadFile(self, request_iterator, context):
        file_path = None  # Initialize file_path as None to be set on the first request
        for request in request_iterator:
            if file_path is None:  # Set the file_path from the first request
                file_path = os.path.join("uploads", request.filename)
                with open(file_path, "wb") as f:
                    # Start writing data from the first request
                    f.write(request.buffer)
            else:
                # Open the file in append mode for subsequent requests
                with open(file_path, "ab") as f:
                    f.write(request.buffer)

        return chord_pb2.UploadFileResponse(message=f"File uploaded successfully at node with ID = {self.node.node_id}.")

    def Gossip(self, request, context):
        message = request.message
        message_id = request.message_id
        if message_id in self.node.received_gossip_message_ids:
            return chord_pb2.Empty()
        self.node.received_gossip_message_ids.add(message_id)
        print(f"Node with port: {self.node.port} received message: {message}")
        self.node.perform_gossip(message_id, message)
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

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        chord_pb2_grpc.add_ChordServiceServicer_to_server(
            ChordNodeServicer(chord_node), server)
        server.add_insecure_port(f"[::]:{node_port}")
        server.start()
        chord_node.join_chord_ring(bootstrap_node)

        print(
            f"Server started at {node_ip_address}:{node_port} with ID {node_id}")

        def run_input_loop():
            while True:
                inp = input(
                    "Select an option:\n1. Print Finger Table\n2. Print Successor\n3. Print Predecessor\n4. Leave chord ring\n5. Set Key\n6. Get Key\n7. Show Store\n8. Download File\n9. Upload File\n10. Gossip\n11. Quit\n")
                if inp == "1":
                    chord_node.show_finger_table()
                elif inp == "2":
                    print(chord_node.successor)
                elif inp == "3":
                    print(chord_node.predecessor)
                elif inp == "4":
                    chord_node.leave()
                    break
                elif inp == "5":
                    key = input("Enter the key to set: ")
                    # Assuming set_key is the correct method
                    result = chord_node.set(key)
                    print("Key set. Node ID:", result.id)
                elif inp == "6":
                    key = input("Enter the key to get: ")
                    result = chord_node.get(key)
                    print("Key found at node ID:", result)
                elif inp == "7":
                    chord_node.show_store()
                    print(chord_node.received_gossip_message_ids)
                elif inp == "8":
                    key = input("Enter the filename to download: ")
                    get_result = chord_node.get(key)
                    file_location_port = get_result.port
                    file_location_ip = get_result.ip
                    if file_location_port is None:
                        print("File not found.")
                    else:
                        print("NEED TO DOWNLOAD FROM NODE", get_result)
                        download_file(key, file_location_ip ,file_location_port)
                elif inp == "9":
                    file_path = input("Enter the filename to upload: ")
                    if os.path.exists(file_path):
                        upload_response = chord_node.upload_file(file_path)
                        print(upload_response.message)
                    else:
                        print("File not found.")
                elif inp == "10":
                    message = input("Enter the message to gossip: ")
                    chord_node.perform_gossip(uuid.uuid4().hex, message)
                elif inp == "11":
                    print("Shutting down the server.")
                    break
                else:
                    print("Invalid option. Please try again.")
            server.stop(0)
        # stabilization(chord_node)
        stabilization_thread = threading.Thread(
            target=run_stabilization, args=(chord_node,))
        # Optional: makes this thread a daemon so it won't prevent the program from exiting
        stabilization_thread.daemon = True
        stabilization_thread.start()
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
