import hashlib
import grpc
from proto import chord_pb2_grpc


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