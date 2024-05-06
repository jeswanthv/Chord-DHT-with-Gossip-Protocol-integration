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
    parser = argparse.ArgumentParser(description="chord Node")
    parser.add_argument("--ip", type=str, help="IP address of the node")
    parser.add_argument("--port", type=int, help="Port number of the node")
    parser.add_argument(
        "--m", type=int, help="Number of bits in the hash space")
    parser.add_argument("--bootstrap_ip", type=str,
                        help="IP address of the bootstrap node", required=False)
    parser.add_argument("--bootstrap_port", type=int,
                        help="Port number of the bootstrap node", required=False)
    return parser.parse_args()

def is_in_between(num, lower_bound, upper_bound, boundary_type):
        # todo implementation 1 - possibly wrong
    # if lower_bound <= upper_bound:
    #     # Normal range (not wrapping around the modulus)
    #     if boundary_type == 'closed':
    #         return lower_bound <= num <= upper_bound
    #     elif boundary_type == 'right_open':
    #         return lower_bound <= num < upper_bound
    #     elif boundary_type == 'left_open':
    #         return lower_bound < num <= upper_bound
    # else:
    #     # Range wraps around the modulus, e.g., (350, 10) in a circle of 0-359
    #     if boundary_type == 'closed':
    #         return num >= lower_bound or num <= upper_bound
    #     elif boundary_type == 'right_open':
    #         return num >= lower_bound or num < upper_bound
    #     elif boundary_type == 'left_open':
    #         return num > lower_bound or num <= upper_bound

    #       todo - Implementation 2 , need to check it and verify it

        lower = lower_bound
        higher = upper_bound

        if lower == num and (boundary_type == 'left_open' or boundary_type == 'closed'):
            # if debug:
            #     print(True)
            return True

        if higher == num and (boundary_type == 'right_open' or boundary_type == 'closed'):
            # if debug:
            #     print(True)
            return True

        in_between_flag = None
        if lower == higher:
            in_between_flag = True
        else:
            if lower < higher:
                in_between_flag = (lower < num) and (num < higher)
            else:
                in_between_flag = not ((higher < num) and (num < lower))

        right_touch_flag = (num == lower) and not ((boundary_type == 'closed') or (boundary_type == 'left_open'))
        left_touch_flag = (num == higher) and not ((boundary_type == 'closed') or (boundary_type == 'right_open'))

        return_type = in_between_flag and not (right_touch_flag or left_touch_flag)
        # if debug:
        #     print(return_type)
        return return_type
