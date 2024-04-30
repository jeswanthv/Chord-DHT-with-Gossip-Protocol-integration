import server_pb2_grpc
import server_pb2
import grpc
import time
from google.protobuf.empty_pb2 import Empty

def getNodeStatus(addr):
    print("Trying to contact node with ip ",addr)
    try:
        with grpc.insecure_channel(addr) as channel:
            stub = server_pb2_grpc.ServerStub(channel)
            find_request = Empty()
            print('Client2.py getNodeStatus emptu request is ',find_request)
            find_resp = stub.get_node_status(find_request)
            print("Client2.py get node status the response after getting the response is",find_resp)
            print("Node ID:\t", find_resp.id)
            print("IP:\t", find_resp.ip)
            print("Predecessor ID:\t", find_resp.pred_id)
            print("Predecessor IP:\t", find_resp.pred_ip)
            print("Successor List:")
            for id, ip in zip(find_resp.suclist_id, find_resp.suclist_ip):
                print(id, ip)
            print("Finger Table:")
            for id, ip in zip(find_resp.finger_id, find_resp.finger_ip):
                print(id, ip)
    except grpc.RpcError as e:
        print(f"Failed to retrieve node status from {addr}: {str(e)}")

def find_successor(addr, key):
    try:
        with grpc.insecure_channel(addr) as channel:
            stub = server_pb2_grpc.ServerStub(channel)
            find_request = server_pb2.FindSucRequest(id=key, inc_id=0, inc_ip="127.0.0.1:7000")
            find_resp = stub.find_successor(find_request)
            print("Successor ID:", find_resp.id)
            print("Successor IP:", find_resp.ip)
    except grpc.RpcError as e:
        print(f"Failed to find successor for key {key} on {addr}: {str(e)}")

def run(Addr):
    try:
        with grpc.insecure_channel(Addr) as channel:
            stub = server_pb2_grpc.ServerStub(channel)
            myKey = "1"*1024
            myValue = "1"*1024
            start = time.time()
            # Simulate 100 PUT operations
            for _ in range(100):
                stub.put(server_pb2.PutRequest(key=myKey, value=myValue))
            print("Time for 100 PUTs:", time.time() - start)

            start = time.time()
            # Perform a GET operation
            stub.get(server_pb2.GetRequest(key=myKey))
            print("Time for GET:", time.time() - start)
    except grpc.RpcError as e:
        print(f"Error during gRPC communication: {str(e)}")

if __name__ == "__main__":
    getNodeStatus("127.0.0.1:7001")
    # find_successor("127.0.0.1:7001", 123)
    run("127.0.0.1:7001")
