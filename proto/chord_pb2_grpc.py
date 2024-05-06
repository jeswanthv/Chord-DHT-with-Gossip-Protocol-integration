# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings

from proto import chord_pb2 as proto_dot_chord__pb2

GRPC_GENERATED_VERSION = '1.63.0'
GRPC_VERSION = grpc.__version__
EXPECTED_ERROR_RELEASE = '1.65.0'
SCHEDULED_RELEASE_DATE = 'June 25, 2024'
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    warnings.warn(
        f'The grpc package installed is at version {GRPC_VERSION},'
        + f' but the generated code in proto/chord_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
        + f' This warning will become an error in {EXPECTED_ERROR_RELEASE},'
        + f' scheduled for release on {SCHEDULED_RELEASE_DATE}.',
        RuntimeWarning
    )


class ChordServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetSuccessor = channel.unary_unary(
                '/chord.ChordService/GetSuccessor',
                request_serializer=proto_dot_chord__pb2.Empty.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.NodeInfo.FromString,
                _registered_method=True)
        self.GetSuccessorList = channel.unary_unary(
                '/chord.ChordService/GetSuccessorList',
                request_serializer=proto_dot_chord__pb2.Empty.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.SuccessorListResponse.FromString,
                _registered_method=True)
        self.GetPredecessor = channel.unary_unary(
                '/chord.ChordService/GetPredecessor',
                request_serializer=proto_dot_chord__pb2.Empty.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.NodeInfo.FromString,
                _registered_method=True)
        self.FindSuccessor = channel.unary_unary(
                '/chord.ChordService/FindSuccessor',
                request_serializer=proto_dot_chord__pb2.FindSuccessorRequest.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.NodeInfo.FromString,
                _registered_method=True)
        self.FindPredecessor = channel.unary_unary(
                '/chord.ChordService/FindPredecessor',
                request_serializer=proto_dot_chord__pb2.FindPredecessorRequest.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.NodeInfo.FromString,
                _registered_method=True)
        self.SetPredecessor = channel.unary_unary(
                '/chord.ChordService/SetPredecessor',
                request_serializer=proto_dot_chord__pb2.NodeInfo.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.Empty.FromString,
                _registered_method=True)
        self.SetSuccessor = channel.unary_unary(
                '/chord.ChordService/SetSuccessor',
                request_serializer=proto_dot_chord__pb2.NodeInfo.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.Empty.FromString,
                _registered_method=True)
        self.UpdateFingerTable = channel.unary_unary(
                '/chord.ChordService/UpdateFingerTable',
                request_serializer=proto_dot_chord__pb2.UpdateFingerTableRequest.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.Empty.FromString,
                _registered_method=True)
        self.FindClosestPrecedingFinger = channel.unary_unary(
                '/chord.ChordService/FindClosestPrecedingFinger',
                request_serializer=proto_dot_chord__pb2.FindClosestPrecedingFingerRequest.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.NodeInfo.FromString,
                _registered_method=True)
        self.GetTransferData = channel.unary_unary(
                '/chord.ChordService/GetTransferData',
                request_serializer=proto_dot_chord__pb2.GetTransferDataRequest.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.GetTransferDataResponse.FromString,
                _registered_method=True)
        self.GetKey = channel.unary_unary(
                '/chord.ChordService/GetKey',
                request_serializer=proto_dot_chord__pb2.GetKeyRequest.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.NodeInfo.FromString,
                _registered_method=True)
        self.SetKey = channel.unary_unary(
                '/chord.ChordService/SetKey',
                request_serializer=proto_dot_chord__pb2.SetKeyRequest.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.NodeInfo.FromString,
                _registered_method=True)
        self.ReceiveKeysBeforeLeave = channel.unary_unary(
                '/chord.ChordService/ReceiveKeysBeforeLeave',
                request_serializer=proto_dot_chord__pb2.ReceiveKeysBeforeLeaveRequest.SerializeToString,
                response_deserializer=proto_dot_chord__pb2.Empty.FromString,
                _registered_method=True)


class ChordServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def GetSuccessor(self, request, context):
        """RPC to get the successor of a node
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetSuccessorList(self, request, context):
        """RPC to get the successor list of a node
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetPredecessor(self, request, context):
        """RPC to get the predecessor of a node
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def FindSuccessor(self, request, context):
        """RPC to find the successor of a node
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def FindPredecessor(self, request, context):
        """RPC to find the predecessor of a node
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SetPredecessor(self, request, context):
        """RPC to update the predecessor of a node
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SetSuccessor(self, request, context):
        """RPC to update the successor of a node
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpdateFingerTable(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def FindClosestPrecedingFinger(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetTransferData(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetKey(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SetKey(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ReceiveKeysBeforeLeave(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ChordServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetSuccessor': grpc.unary_unary_rpc_method_handler(
                    servicer.GetSuccessor,
                    request_deserializer=proto_dot_chord__pb2.Empty.FromString,
                    response_serializer=proto_dot_chord__pb2.NodeInfo.SerializeToString,
            ),
            'GetSuccessorList': grpc.unary_unary_rpc_method_handler(
                    servicer.GetSuccessorList,
                    request_deserializer=proto_dot_chord__pb2.Empty.FromString,
                    response_serializer=proto_dot_chord__pb2.SuccessorListResponse.SerializeToString,
            ),
            'GetPredecessor': grpc.unary_unary_rpc_method_handler(
                    servicer.GetPredecessor,
                    request_deserializer=proto_dot_chord__pb2.Empty.FromString,
                    response_serializer=proto_dot_chord__pb2.NodeInfo.SerializeToString,
            ),
            'FindSuccessor': grpc.unary_unary_rpc_method_handler(
                    servicer.FindSuccessor,
                    request_deserializer=proto_dot_chord__pb2.FindSuccessorRequest.FromString,
                    response_serializer=proto_dot_chord__pb2.NodeInfo.SerializeToString,
            ),
            'FindPredecessor': grpc.unary_unary_rpc_method_handler(
                    servicer.FindPredecessor,
                    request_deserializer=proto_dot_chord__pb2.FindPredecessorRequest.FromString,
                    response_serializer=proto_dot_chord__pb2.NodeInfo.SerializeToString,
            ),
            'SetPredecessor': grpc.unary_unary_rpc_method_handler(
                    servicer.SetPredecessor,
                    request_deserializer=proto_dot_chord__pb2.NodeInfo.FromString,
                    response_serializer=proto_dot_chord__pb2.Empty.SerializeToString,
            ),
            'SetSuccessor': grpc.unary_unary_rpc_method_handler(
                    servicer.SetSuccessor,
                    request_deserializer=proto_dot_chord__pb2.NodeInfo.FromString,
                    response_serializer=proto_dot_chord__pb2.Empty.SerializeToString,
            ),
            'UpdateFingerTable': grpc.unary_unary_rpc_method_handler(
                    servicer.UpdateFingerTable,
                    request_deserializer=proto_dot_chord__pb2.UpdateFingerTableRequest.FromString,
                    response_serializer=proto_dot_chord__pb2.Empty.SerializeToString,
            ),
            'FindClosestPrecedingFinger': grpc.unary_unary_rpc_method_handler(
                    servicer.FindClosestPrecedingFinger,
                    request_deserializer=proto_dot_chord__pb2.FindClosestPrecedingFingerRequest.FromString,
                    response_serializer=proto_dot_chord__pb2.NodeInfo.SerializeToString,
            ),
            'GetTransferData': grpc.unary_unary_rpc_method_handler(
                    servicer.GetTransferData,
                    request_deserializer=proto_dot_chord__pb2.GetTransferDataRequest.FromString,
                    response_serializer=proto_dot_chord__pb2.GetTransferDataResponse.SerializeToString,
            ),
            'GetKey': grpc.unary_unary_rpc_method_handler(
                    servicer.GetKey,
                    request_deserializer=proto_dot_chord__pb2.GetKeyRequest.FromString,
                    response_serializer=proto_dot_chord__pb2.NodeInfo.SerializeToString,
            ),
            'SetKey': grpc.unary_unary_rpc_method_handler(
                    servicer.SetKey,
                    request_deserializer=proto_dot_chord__pb2.SetKeyRequest.FromString,
                    response_serializer=proto_dot_chord__pb2.NodeInfo.SerializeToString,
            ),
            'ReceiveKeysBeforeLeave': grpc.unary_unary_rpc_method_handler(
                    servicer.ReceiveKeysBeforeLeave,
                    request_deserializer=proto_dot_chord__pb2.ReceiveKeysBeforeLeaveRequest.FromString,
                    response_serializer=proto_dot_chord__pb2.Empty.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'chord.ChordService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ChordService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def GetSuccessor(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/GetSuccessor',
            proto_dot_chord__pb2.Empty.SerializeToString,
            proto_dot_chord__pb2.NodeInfo.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetSuccessorList(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/GetSuccessorList',
            proto_dot_chord__pb2.Empty.SerializeToString,
            proto_dot_chord__pb2.SuccessorListResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetPredecessor(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/GetPredecessor',
            proto_dot_chord__pb2.Empty.SerializeToString,
            proto_dot_chord__pb2.NodeInfo.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def FindSuccessor(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/FindSuccessor',
            proto_dot_chord__pb2.FindSuccessorRequest.SerializeToString,
            proto_dot_chord__pb2.NodeInfo.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def FindPredecessor(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/FindPredecessor',
            proto_dot_chord__pb2.FindPredecessorRequest.SerializeToString,
            proto_dot_chord__pb2.NodeInfo.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def SetPredecessor(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/SetPredecessor',
            proto_dot_chord__pb2.NodeInfo.SerializeToString,
            proto_dot_chord__pb2.Empty.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def SetSuccessor(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/SetSuccessor',
            proto_dot_chord__pb2.NodeInfo.SerializeToString,
            proto_dot_chord__pb2.Empty.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def UpdateFingerTable(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/UpdateFingerTable',
            proto_dot_chord__pb2.UpdateFingerTableRequest.SerializeToString,
            proto_dot_chord__pb2.Empty.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def FindClosestPrecedingFinger(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/FindClosestPrecedingFinger',
            proto_dot_chord__pb2.FindClosestPrecedingFingerRequest.SerializeToString,
            proto_dot_chord__pb2.NodeInfo.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetTransferData(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/GetTransferData',
            proto_dot_chord__pb2.GetTransferDataRequest.SerializeToString,
            proto_dot_chord__pb2.GetTransferDataResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def GetKey(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/GetKey',
            proto_dot_chord__pb2.GetKeyRequest.SerializeToString,
            proto_dot_chord__pb2.NodeInfo.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def SetKey(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/SetKey',
            proto_dot_chord__pb2.SetKeyRequest.SerializeToString,
            proto_dot_chord__pb2.NodeInfo.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def ReceiveKeysBeforeLeave(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/chord.ChordService/ReceiveKeysBeforeLeave',
            proto_dot_chord__pb2.ReceiveKeysBeforeLeaveRequest.SerializeToString,
            proto_dot_chord__pb2.Empty.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)
