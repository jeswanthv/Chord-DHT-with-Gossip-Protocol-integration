import hashlib
import grpc
from proto import chord_pb2_grpc
import json
import argparse


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



def is_in_between(num, min_limit, max_limit, type='c'):
    """
    Author: Ishan Goel
    Helper function. Implements belongs_to mathematical function in context of a modulus style chord ring.
    :param num: id
    :param limits: boundaries (tuple of two elements)
    :param type: c: closed, r: right closed, l:left closed
    :return: bool
    """
    # min_limit, max_limit = limits
    if min_limit <= max_limit:
        # Normal range (not wrapping around the modulus)
        if type == 'c':
            return min_limit <= num <= max_limit
        elif type == 'r':
            return min_limit <= num < max_limit
        elif type == 'l':
            return min_limit < num <= max_limit
    else:
        # Range wraps around the modulus, e.g., (350, 10) in a circle of 0-359
        if type == 'c':
            return num >= min_limit or num <= max_limit
        elif type == 'r':
            return num >= min_limit or num < max_limit
        elif type == 'l':
            return num > min_limit or num <= max_limit
