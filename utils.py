import os
import hashlib
import grpc
from proto import chord_pb2_grpc
import json
import argparse
from proto import chord_pb2
from prettytable import PrettyTable, ALL
import logging

os.environ['GRPC_VERBOSITY'] = 'NONE'

def sha1_hash(key, m):
    """
    Function to hash the key using sha1 and return the hash value truncated to m bits
    Args:
        key: Key to be hashed
        m: Number of bits to truncate the hash value
    Returns:
        Hash value truncated to m bits
    """
    return int(hashlib.sha1(str(key).encode()).hexdigest(), 16) % (2**m)


def create_stub(ip, port):
    """
    Helper method to create and return a gRPC stub.
    Uses a context manager to ensure the channel is closed after use.
    """
    channel = grpc.insecure_channel(f'{ip}:{port}')
    return chord_pb2_grpc.ChordServiceStub(channel), channel


# def create_stub(ip, port, cert_file='certs/server.crt'):
    # """
    # Helper method to create and return a gRPC stub.
    # Uses a context manager to ensure the channel is closed after use.
    # The communication is secured using SSL/TLS.

    # Args:
    #     ip (str): The IP address of the server.
    #     port (int): The port on which the server is running.
    #     cert_file (str): Path to the SSL certificate file for secure communication.

    # Returns:
    #     tuple: A tuple containing the gRPC stub and the channel.
    # """
    # # Load the server's certificate
    # with open(cert_file, 'rb') as f:
    #     trusted_certs = f.read()

    # # Create SSL credentials using the loaded certificate
    # credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)

    # # Create a secure channel with the credentials
    # channel = grpc.secure_channel(f'{ip}:{port}', credentials)

    # # Create a stub from the channel
    # stub = chord_pb2_grpc.ChordServiceStub(channel)

    # return stub, channel


def get_args():
    parser = argparse.ArgumentParser(description="Chord Node")
    parser.add_argument("--ip", type=str, help="IP address of the node")
    parser.add_argument("--port", type=int, help="Port number of the node")
    parser.add_argument(
        "--m", type=int, help="Number of bits in the hash space")
    parser.add_argument("--bootstrap_ip", type=str,
                        help="IP address of the bootstrap node", required=False)
    parser.add_argument("--bootstrap_port", type=int,
                        help="Port number of the bootstrap node", required=False)
    parser.add_argument("-i", "--interactive", action="store_true")
    return parser.parse_args()


def is_within_bounds(value, start, end, inclusion_type='c'):

    if start == value and (inclusion_type == 'l' or inclusion_type == 'c'):
        return True

    if end == value and (inclusion_type == 'r' or inclusion_type == 'c'):
        return True

    is_within = None
    if start == end:
        is_within = True
    else:
        if start < end:
            is_within = (start < value) and (value < end)
        else:
            is_within = not ((end < value) and (value < start))

    touches_right_boundary = (value == start) and not (
        (inclusion_type == 'c') or (inclusion_type == 'l'))
    touches_left_boundary = (value == end) and not (
        (inclusion_type == 'c') or (inclusion_type == 'r'))

    result = is_within and not (
        touches_right_boundary or touches_left_boundary)
    return result


def download_file(file_name, ip, port):
    # using grpc
    try:
        stub, channel = create_stub(ip, port)

        with channel:
            response = stub.DownloadFile(
                chord_pb2.DownloadFileRequest(filename=file_name))
            file_name = os.path.join("downloads", file_name)
            
            with open(file_name, 'wb') as f:
                for r in response:
                    f.write(r.buffer)

            print(f"File {file_name} downloaded successfully from node {port}")
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.NOT_FOUND:
            print(f"Failed to download file: {file_name}. Server message: {e.details()}")
    except Exception as e:
        pass


def generate_requests(file_path):
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)  # Read in chunks of 1 MB
            if not chunk:
                break
            yield chord_pb2.UploadFileRequest(filename=os.path.basename(file_path), buffer=chunk)


def menu_options():
    table = [["No.", "Option"], ["1", "Display Finger Table"], ["2", "Display Successor"], ["3", "Display Predecessor"], [
        "4", "Show Store"], ["5", "Upload File"], ["6", "Download File"], ["7", "Perform Gossip"], ["8", "Leave Chord Ring (Graceful)"], ["9", "Shut Down Peer"]]

    tab = PrettyTable(table[0])
    tab.add_rows(table[1:])
    tab.hrules = ALL
    return tab


def load_ssl_credentials():
    # Load the server's key and certificate files
    with open('certs/server.key', 'rb') as f:
        private_key = f.read()
    with open('certs/server.crt', 'rb') as f:
        certificate_chain = f.read()

    # Create a server credentials object
    server_credentials = grpc.ssl_server_credentials(
        ((private_key, certificate_chain,),))

    return server_credentials


logger = logging.getLogger(__name__)
logging.basicConfig(filename='chord_ring_activity.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s -> %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
