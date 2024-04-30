from concurrent import futures
import grpc
import os
import proto_buffers.data_struct_pb2 as data_struct_pb2
import proto_buffers.data_struct_pb2_grpc as data_struct_pb2_grpc

class FileService(data_struct_pb2_grpc.FileServiceServicer):
    
    def RequestFile(self, request, context):

        peer_info = context.peer()
        print(f"Received a request from client address: {peer_info}")
        file_path = os.path.join('/Users/suryakangeyan/Projects/Distributed Computing final Project/server/files', request.filename)
        try:
            with open(file_path, 'rb') as f:
                while True:
                    piece = f.read(1024)  # Read file in chunks of 1024 bytes
                    if not piece:
                        break
                    yield data_struct_pb2.FileResponse(data=piece)
        except FileNotFoundError:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('File not found')
            return

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_struct_pb2_grpc.add_FileServiceServicer_to_server(FileService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server listening on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()