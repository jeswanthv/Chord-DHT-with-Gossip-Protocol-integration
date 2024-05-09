from prettytable import PrettyTable, ALL
from utils import download_file, create_stub, generate_requests
import uuid
from proto import chord_pb2

def menu_options():
    table = [["No.", "Option"], [1, "Download File"], [
        "2", "Upload File"], ["3", "Perform Gossip"], ["4", "Exit"]]

    tab = PrettyTable(table[0])
    tab.add_rows(table[1:])
    tab.hrules = ALL
    return tab

def upload_file(file_path, ip, port):
    stub, channel = create_stub(ip, port)

    with channel:
        response = stub.UploadFile(generate_requests(file_path), timeout = 20)
        print(f"File {file_path} uploaded successfully to node {port}")
        print(response)

def perform_gossip(ip, port, message):
    stub, channel = create_stub(ip, port)

    with channel:
        gossip_request = chord_pb2.GossipRequest(
                    message=message, message_id=uuid.uuid4().hex)
        response = stub.Gossip(gossip_request)
        
        print(f"Gossip performed successfully on node {port}")
        print(response)

port = input("Enter the port number: ")
ip = input("Enter the IP address: ")

while True:
    print(menu_options())
    choice = input("Enter your choice: ")

    if choice == "1":
        file_name = input("Enter the file name: ")
        download_file(file_name, ip, port)
    elif choice == "2":
        file_path = input("Enter the file path: ")
        upload_file(file_path, ip, port)
    elif choice == "3":
        message = input("Enter the message: ")
        perform_gossip(ip, port, message)
    elif choice == "4":
        break
    else:
        print("Invalid choice. Please try again.")