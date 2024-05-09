from prettytable import PrettyTable, ALL
from utils import download_file, create_stub, generate_requests
import uuid
from proto import chord_pb2
import time
from chord.node import Node
import os
os.environ['GRPC_VERBOSITY'] = 'NONE'


def menu_options():
    table = [["No.", "Option"], ["1", "Download File"], [
        "2", "Upload File"], ["3", "Perform Gossip"], ["4", "Performance"], ["5", "Exit"]]

    tab = PrettyTable(table[0])
    tab.add_rows(table[1:])
    tab.hrules = ALL
    return tab

def perform_gossip(ip, port, message):
    stub, channel = create_stub(ip, port)

    with channel:
        gossip_request = chord_pb2.GossipRequest(
            message=message, message_id=uuid.uuid4().hex)
        response = stub.Gossip(gossip_request)

        print(f"Gossip performed successfully on node at {ip}:{port}")
        print(response)


def performance(chord_node, ip, port):
    start = time.time()
    chord_node.upload_file("requirements.txt", ip, port)
    end = time.time()
    print(f"Upload time: {end - start}")


ip = input("Enter the IP address of bootstrap node: ")
port = input("Enter the port number of bootstrap node: ")

while True:
    print(menu_options())
    choice = input("Enter your choice: ")
    chord_node = Node(0, ip, port, 0)  # dummy node - only used to get the key
    if choice == "1":
        file_name = input("Enter the file name to download: ")
        get_result = chord_node.get(file_name)
        file_location_port = get_result.port
        file_location_ip = get_result.ip
        print(get_result)
        if not file_location_ip or not file_location_port:
            print("File not found in chord ring!")
        else:
            print(
                f"File Found At Node: {file_location_ip}:{file_location_port}")
            download_file(file_name, file_location_ip, file_location_port)
    elif choice == "2":
        file_path = input("Enter the filename to upload: ")
        if os.path.exists(file_path):
            upload_response = chord_node.upload_file(file_path)
            print(upload_response.message)
        else:
            print("File not found.")
    elif choice == "3":
        message = input("Enter the gossip message: ")
        perform_gossip(ip, port, message)
    elif choice == "4":
        performance(chord_node, ip, port)
    elif choice == "5":
        break
    else:
        print("Invalid choice. Please try again.")
