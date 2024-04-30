import grpc
from concurrent import futures
import threading
import time
import hashlib
import json
import os
import logging

# Assuming server_pb2 and server_pb2_grpc are the generated files from your .proto definitions
import server_pb2
import server_pb2_grpc


class VirtualNode(server_pb2_grpc.ServerServicer):
    def __init__(self, id, port, config):
        self.id = id
        self.port = port
        self.config = config
        self.server = None
        self.logger = logging.getLogger(f"Node{self.id}")
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def start_server(self):
        """Starts the gRPC server and listens on the specified port."""
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        server_pb2_grpc.add_ServerServicer_to_server(self, self.server)
        self.server.add_insecure_port(f'localhost:{self.port}')
        self.server.start()
        self.logger.info(f"Node {self.id} listening on port {self.port}")
        try:
            self.server.wait_for_termination()
        except KeyboardInterrupt:
            self.server.stop(1000)
    def report_status(self):
        """Periodically reports the status of this node."""
        while True:
            self.logger.info(f"Node {self.id} running on port {self.port}")
            time.sleep(2)

    def find_successor(self, request, context):
        requested_id = request.id

        # Log the incoming request
        self.logger.debug(f'Received find_successor request for ID: {requested_id}')

        # Case where only one node is in the network or request ID is the node's own ID
        if self.id == self.successor_list[0][0] or requested_id == self.id:
            self.logger.debug('Single node in network or request ID matches own ID.')
            return server_pb2.FindSucResponse(id=self.id, ip=self.local_addr)

        # If the requested ID is in the interval (self.id, self.successor_list[0][0])
        if self.between(self.id, requested_id, self.successor_list[0][0]):
            self.logger.debug(f'Successor for ID {requested_id} is {self.successor_list[0][0]}')
            return server_pb2.FindSucResponse(id=self.successor_list[0][0], ip=self.successor_list[0][1])

        # If not found, find the closest preceding node and ask it for the successor
        closest_preceding = self.closest_preceding_node(requested_id)
        self.logger.debug(
            f'Closest preceding node for ID {requested_id} is {closest_preceding[0]} at {closest_preceding[1]}')

        # Create a gRPC stub for the closest preceding node
        channel = grpc.insecure_channel(closest_preceding[1])
        stub = server_pb2_grpc.ServerStub(channel)

        # Recursive find_successor call to the closest preceding node
        try:
            response = stub.find_successor(server_pb2.FindSucRequest(id=requested_id), timeout=self.GLOBAL_TIMEOUT)
            return response
        except grpc.RpcError as e:
            self.logger.error(f'RPC failed for find_successor from node {closest_preceding[0]}: {e}')
            # Handle failure (e.g., node failure), perhaps by retrying or escalating the issue
            raise

    def get(self, request, context):
        client_ip = context.peer()  # Retrieves the IP of the calling client
        get_resp = server_pb2.GetResponse()
        hash_val = self.sha1(request.key, self.SIZE)

        # Log the request details
        self.logger.info(f'Received GET request from {client_ip} for key: {request.key}')

        if not self.between(self.predecessor[0], hash_val, self.id):
            find_suc_resp = self.init_find_successor(hash_val, self.successor_list[0][1])
            get_resp.ret = server_pb2.FAILURE
            get_resp.response = ""
            get_resp.nodeID = find_suc_resp.id
            get_resp.nodeIP = find_suc_resp.ip
        else:
            try:
                get_resp.ret = server_pb2.SUCCESS
                get_resp.response = self.state_machine[request.key]
                get_resp.nodeID = -1
                get_resp.nodeIP = ""
            except KeyError:
                get_resp.ret = server_pb2.FAILURE
                get_resp.response = "N/A"
                get_resp.nodeID = -1
                get_resp.nodeIP = ""
        return get_resp

    def put(self, request, context):
        client_ip = context.peer()  # Retrieves the IP of the calling client
        put_resp = server_pb2.PutResponse()
        hash_val = self.sha1(request.key, self.SIZE)

        # Log the request details
        self.logger.info(f'Received PUT request from {client_ip} for key: {request.key} with value: {request.value}')

        if not self.between(self.predecessor[0], hash_val, self.id):
            find_suc_resp = self.init_find_successor(hash_val, self.successor_list[0][1])
            put_resp.ret = server_pb2.FAILURE
            put_resp.nodeID = find_suc_resp.id
            put_resp.nodeIP = find_suc_resp.ip
        else:
            self.logs.append([request.key, request.value])
            self.state_machine[request.key] = request.value
            put_resp.ret = server_pb2.SUCCESS
            put_resp.nodeID = -1
            put_resp.nodeIP = ""
            replicate_req = server_pb2.ReplicateRequest()
            entry = replicate_req.entries.add()
            entry.hashID = hash_val
            entry.key = request.key
            entry.val = request.value
            self.send_replicate_entries(replicate_req)
        return put_resp

    def get_node_status(self, request, context):
        print("VirtualNode get NOde status called for node with port id",request)
        client_ip = context.peer()  # Retrieves the IP of the calling client
        self.logger.info(f'Node status requested by {client_ip}')

        resp = server_pb2.NodeStatus()
        resp.id = self.id
        resp.ip = self.local_addr
        resp.pred_id = self.predecessor[0]
        resp.pred_ip = self.predecessor[1]
        for i in range(len(self.successor_list)):
            resp.suclist_id.append(self.successor_list[i][0])
            resp.suclist_ip.append(self.successor_list[i][1])
        for i in range(len(self.finger)):
            resp.finger_id.append(self.finger[i][0])
            resp.finger_ip.append(self.finger[i][1])
        return resp


def initialize_nodes(number_of_nodes):
    """Creates and starts a specified number of nodes, each on its own port."""
    config = {
        "replication_num": 3,
        "successor_num": 2,
        "stabilize_period": 1000,
        "fixfinger_period": 1000,
        "checkpre_period": 1000,
        "global_timeout": 500,
        "join_retry_period": 5,
        "num_servers": number_of_nodes,
        "log_size": 10
    }

    initial_port = 7001
    nodes = []
    for i in range(number_of_nodes):
        node = VirtualNode(id=i, port=initial_port + i, config=config)
        nodes.append(node)
        server_thread = threading.Thread(target=node.start_server)
        server_thread.start()

        time.sleep(0.1)  # Give the server time to start
        # server_thread.join()
    # Start a status reporting thread for each node
    # for node in nodes:
    #     status_thread = threading.Thread(target=node.report_status)
    #     status_thread.start()

    return nodes


if __name__ == "__main__":
    nodes = initialize_nodes(10)
