import os
import hashlib
import grpc
from proto import chord_pb2_grpc
import json
import argparse
from proto import chord_pb2
from prettytable import PrettyTable, ALL


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


# def create_stub(ip, port):
#     """
#     Helper method to create and return a gRPC stub.
#     Uses a context manager to ensure the channel is closed after use.
#     """
#     channel = grpc.insecure_channel(f'{ip}:{port}')
#     return chord_pb2_grpc.ChordServiceStub(channel), channel


def create_stub(ip, port, cert_file='server.crt'):
    """
    Helper method to create and return a gRPC stub.
    Uses a context manager to ensure the channel is closed after use.
    The communication is secured using SSL/TLS.
    
    Args:
        ip (str): The IP address of the server.
        port (int): The port on which the server is running.
        cert_file (str): Path to the SSL certificate file for secure communication.

    Returns:
        tuple: A tuple containing the gRPC stub and the channel.
    """
    # Load the server's certificate
    with open(cert_file, 'rb') as f:
        trusted_certs = f.read()

    # Create SSL credentials using the loaded certificate
    credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)

    # Create a secure channel with the credentials
    channel = grpc.secure_channel(f'{ip}:{port}', credentials)

    # Create a stub from the channel
    stub = chord_pb2_grpc.ChordServiceStub(channel)

    return stub, channel


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
    return parser.parse_args()

# def is_in_between(num, lower_bound, upper_bound, boundary_type):

#     if lower_bound <= upper_bound:
#         # Normal range (not wrapping around the modulus)
#         if boundary_type == 'closed':
#             return lower_bound <= num <= upper_bound
#         elif boundary_type == 'right_open':
#             return lower_bound <= num < upper_bound
#         elif boundary_type == 'left_open':
#             return lower_bound < num <= upper_bound
#     else:
#         # Range wraps around the modulus, e.g., (350, 10) in a circle of 0-359
#         if boundary_type == 'closed':
#             return num >= lower_bound or num <= upper_bound
#         elif boundary_type == 'right_open':
#             return num >= lower_bound or num < upper_bound
#         elif boundary_type == 'left_open':
#             return num > lower_bound or num <= upper_bound


def is_in_between(num, lower, higher, type='c'):
    """
    Author: Ishan Goel
    Helper function. Implements belongs_to mathematical function in context of a modulus style chord ring.
    :param num: id
    :param limits: boundaries (tuple of two elements)
    :param type: c: closed, r: right closed, l:left closed
    :return: bool
    """
    # min_limit, max_limit = limits

    if lower == num and (type == 'l' or type == 'c'):
        return True

    if higher == num and (type == 'r' or type == 'c'):
        return True

    in_between_flag = None
    if lower == higher:
        in_between_flag = True
    else:
        if lower < higher:
            in_between_flag = (lower < num) and (num < higher)
        else:
            in_between_flag = not ((higher < num) and (num < lower))

    right_touch_flag = (num == lower) and not ((type == 'c') or (type == 'l'))
    left_touch_flag = (num == higher) and not ((type == 'c') or (type == 'r'))

    return_type = in_between_flag and not (right_touch_flag or left_touch_flag)
    return return_type


def download_file(file_name, ip, port):
    # using grpc

    stub, channel = create_stub(ip, port)

    with channel:
        response = stub.DownloadFile(
            chord_pb2.DownloadFileRequest(filename=file_name))
        file_name = os.path.join("downloads", file_name)

        with open(file_name, 'wb') as f:
            for r in response:
                f.write(r.buffer)

        print(f"File {file_name} downloaded successfully from node {port}")


def generate_requests(file_path):
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)  # Read in chunks of 1 MB
            if not chunk:
                break
            yield chord_pb2.UploadFileRequest(filename=os.path.basename(file_path), buffer=chunk)


def menu_options():
    table = [["No.", "Option"], [1, "Display Finger Table"], ["2", "Display Successor"], ["3", "Display Predecessor"], ["4", "Leave Chord Ring"], [
        "5", "Set Key"], ["6", "Get Key"], ["7", "Show Store"], ["8", "Download File"], ["9", "Upload File"], ["10", "Perform Gossip"], ["11", "Exit"]]

    tab = PrettyTable(table[0])
    tab.add_rows(table[1:])
    tab.hrules = ALL
    return tab


def load_ssl_credentials():
    # Load the server's key and certificate files
    with open('server.key', 'rb') as f:
        private_key = f.read()
    with open('server.crt', 'rb') as f:
        certificate_chain = f.read()

    # Create a server credentials object
    server_credentials = grpc.ssl_server_credentials(
        ((private_key, certificate_chain,),))
    
    return server_credentials