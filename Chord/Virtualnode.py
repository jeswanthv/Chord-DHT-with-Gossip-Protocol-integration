import grpc
from concurrent import futures
import threading
import time
import logging
import os
import threading
import hashlib
import hashlib
import json
import os
from threading import Condition
from threading import Lock
import logging

# Assuming server_pb2 and server_pb2_grpc are the generated files from your .proto definitions
import server_pb2
import server_pb2_grpc


class VirtualNode(server_pb2_grpc.ServerServicer):
    def __init__(self, id: int, local_addr, remote_addr, config):
        # Read configuration from json file
        self.REP_NUM = config["replication_num"]
        self.SUCCESSOR_NUM = config["successor_num"]
        self.STABLE_PERIOD = config["stabilize_period"]
        self.FIXFINGER_PERIOD = config["fixfinger_period"]
        self.CHECKPRE_PERIOD = config["checkpre_period"]
        self.GLOBAL_TIMEOUT = config["global_timeout"]
        self.JOIN_RETRY_PERIOD = config["join_retry_period"]
        self.NUM_SERVERS = config["num_servers"]
        self.LOG_SIZE = config["log_size"]
        self.SIZE = 1 << self.LOG_SIZE

        self.local_addr = local_addr
        self.remote_addr = remote_addr
        # self.id = sha1(self.local_addr, self.SIZE)
        self.id = id
        self.only_node_phase = -1
        self.second_node = False

        # [server id, server IP]
        self.finger = [[-1, ""] for _ in range(self.LOG_SIZE)]
        self.successor_list = [[-1, ""] for _ in range(self.SUCCESSOR_NUM)]
        self.predecessor = [-1, ""]
        self.next = 0

        # Condition variable related
        self.fix_finger_notified = False
        self.rectify_cond = Condition()
        self.fix_finger_cond = Condition()
        self.check_pred_cond = Condition()
        self.stabilize_cond = Condition()
        self.successor_list_lock = Lock()

        # self.disk_state_machine = "log/state_machine-%d.pkl" % self.id
        self.state_machine = {}
        self.logs = []
        self.last_applied = 0

        # chaos monkey server
        # self.cmserver = CMServer(num_server=self.NUM_SERVERS)

        # Set up Logger, create logger with 'chord'
        self.logger = logging.getLogger("chord")
        self.logger.setLevel(logging.DEBUG)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('[%(asctime)s,%(msecs)d %(levelname)s]: %(message)s',
                                      datefmt='%M:%S')
        # create file handler which logs even debug messages
        os.makedirs(os.path.dirname('log/logger-%d.txt' % self.id), exist_ok=True)
        fh = logging.FileHandler('log/logger-%d.txt' % self.id)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        # create console handler with a higher log level
        # TODO: adjust level here
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)



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

        client_ip = context.peer()  # Retrieves the IP of the calling client
        self.logger.info(f'Node status requested by {client_ip}')
        print(f"VirtualNode get Node status called for node with port id", {client_ip})

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

    def create(self):
        self.only_node_phase = 0
        self.predecessor = [-1, ""]
        self.successor_list[0] = [self.id, self.local_addr]
        self.logger.debug(f"[Create]: Create chord ring, 1st vn id: <{self.id}>, ip: <{self.local_addr}>")
    def run(self):
        # self.logger.debug(f"[Init]: Start virtual node, id is: <{self.id}>")
        if self.remote_addr == self.local_addr:
            self.create()
            # self.logger.debug(f"[Init]: New Chord Ring with vn, id: <{self.id}>, ip: <{self.ip}>")
        # stabilize_th = threading.Thread(target=self.stabilize, args=())
        # stabilize_th.start()
        # fix_finger_th = threading.Thread(target=self.fix_finger, args=())
        # fix_finger_th.start()
        # threading.Thread(target=self.init_rectify, args=()).start()
        if self.local_addr != self.remote_addr:
            print("This is the place where the chord servers running on different ports try to join the bootstrap node ");
            self.join(self.id, self.remote_addr)