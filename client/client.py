import grpc
import os

import prot_buffers.data_struct_pb2 as data_struct_pb2
import prot_buffers.data_struct_pb2_grpc as data_struct_pb2_grpc

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = data_struct_pb2_grpc.FileServiceStub(channel)
        filename = "example.txt"  # Filename to request
        responses = stub.RequestFile(data_struct_pb2.FileRequest(filename=filename))

        # Specify the directory where you want to save the file
        save_directory = "clientpath"
        os.makedirs(save_directory, exist_ok=True)  # Ensure the directory exists

        # Construct the full path where the file will be saved
        save_path = os.path.join(save_directory, f"received_{filename}")

        # Open the file at the specified path and write the data received from the server
        with open(save_path, 'wb') as f:
            for response in responses:
                f.write(response.data)

        print(f"File {filename} received and saved successfully to {save_path}.")

if __name__ == '__main__':
    run()
