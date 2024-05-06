import grpc
from proto import chord_pb2
from proto import chord_pb2_grpc
import threading
from concurrent import futures

from utils import sha1_hash, get_args, create_stub, is_in_between
from chord.node import Node


class ChordNodeServicer(chord_pb2_grpc.ChordServiceServicer):

    def __init__(self, node: Node):
        self.node = node

    def GetSuccessor(self, request, context):

        print("GetSuccessor rpc called with port", self.node.port)
        response = chord_pb2.NodeInfo()
        response.id = self.node.successor.node_id
        response.ip = self.node.successor.ip
        response.port = self.node.successor.port
        print("Returning successor", response)
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
            if intermediate_node is not None:
                intermediate_stub, intermediate_channel = create_stub(
                    intermediate_node.ip, intermediate_node.port)
                with intermediate_channel:
                    get_successor_request = chord_pb2.Empty()
                    get_successor_response = intermediate_stub.GetSuccessor(
                        get_successor_request)
                    self.node.successor_list[i] = Node(
                        get_successor_response.id, get_successor_response.ip, get_successor_response.port, self.node.m)
            else:
                # TODO handle node crash
                pass

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

        print("Finding predecessor rpc called with port", self.node.port)
        # if first node in the ring
        if self.node.successor.node_id == self.node.node_id:
            print("Returning self as predecessor")
            response = chord_pb2.NodeInfo()
            response.id = self.node.node_id
            response.ip = self.node.ip
            response.port = self.node.port
            return response

        else:
            id_to_find = request.id
            current_node = self.node

            while not (is_in_between(id_to_find, current_node.node_id, current_node.successor.node_id, 'right_open')):
                print("INFINITE LOOP", id_to_find, current_node.node_id,
                      current_node.successor.node_id)
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

            print("LOOP BROKEN")
            response = chord_pb2.NodeInfo()
            response.id = current_node.node_id
            response.ip = current_node.ip
            response.port = current_node.port
            return response

    def FindSuccessor(self, request, context):
        """
        FindSuccessor RPC call to find the successor node of a given node_id.
        LINEAR SEARCH IMPLEMENTATION
        """
        print("Finding successor rpc called with port", self.node.port)
        id_to_find = request.id

        # IMPLEMENTATION 1
        # my_id = self.node.node_id
        # my_successor_id = self.node.successor.node_id
        # # Check if the requested ID is in the range (my_id, my_successor_id]
        # if (my_id < id_to_find <= my_successor_id) or (my_id > my_successor_id and (id_to_find > my_id or id_to_find <= my_successor_id)):
        #     # The current node's successor is the successor of the requested node_id
        #     response = chord_pb2.NodeInfo()
        #     response.node_id = self.node.successor.node_id
        #     response.ip_address = self.node.successor.ip
        #     response.port = self.node.successor.port
        #     return response
        # else:
        #     # Need to ask the successor to find the successor
        #     channel = grpc.insecure_channel(
        #         f'{self.node.successor.ip}:{self.node.successor.port}')
        #     stub = chord_pb2_grpc.ChordServiceStub(channel)
        #     successor_request = chord_pb2.FindSuccessorRequest(id=id_to_find)
        #     return stub.FindSuccessor(successor_request)

        # IMPLEMENTATION 2
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
        print("FindClosestPrecedingFinger rpc called with port", self.node.port)
        # print("self.node.node_id", self.node.node_id)
        # print("CLOSEST PRECEDING FINGER ID", id_to_find)
        # print("FINGER TABLE", self.node.finger_table[9].node_id)
        for i in range(self.node.m-1, -1, -1):
            if is_in_between(self.node.finger_table[i].node_id, self.node.node_id, id_to_find, 'closed'):
                # print("TRUE FOR IDENTIFIER", id_to_find)
                print("i=", i, self.node.finger_table[i], [
                      self.node.node_id, id_to_find])

                response.id = self.node.finger_table[i].node_id
                response.ip = self.node.finger_table[i].ip
                response.port = self.node.finger_table[i].port
                return response

        response.id = self.node.node_id
        response.ip = self.node.ip
        response.port = self.node.port
        return response

    def UpdateFingerTable(self, request, context):
        print("UpdateFingerTable rpc called with port", self.node.port)
        i = request.i
        source_node = request.node
        source_id = source_node.id
        source_ip = source_node.ip
        source_port = source_node.port
        for_leave = request.for_leave

        if for_leave:
            self.node.finger_table[i] = Node(
                id, source_ip, source_port, self.node.m)
            return chord_pb2.Empty()

        if source_id != self.node.node_id and is_in_between(source_id, self.node.node_id, self.node.finger_table[i].node_id, 'left_open'):
            self.node.finger_table[i] = Node(
                source_id, source_ip, source_port, self.node.m)
            # Ask the predecessor to update the finger table
            pred = self.node.predecessor

            while True:
                try:
                    print("AAA")
                    if not pred or pred.node_id == self.node.node_id:
                        print("BBB")
                        pred = self
                    else:
                        print("CCC")
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
