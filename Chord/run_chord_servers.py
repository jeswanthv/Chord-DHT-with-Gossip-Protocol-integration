import click
import csv
import os
import socket
import time


def gen_run_script(server_list_file, no_exec):
    servers = []

    # Read server configurations from CSV file
    with open(server_list_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            servers.append(dict(row))

    script = ''
    # # Load a script template from a file (if needed for additional setup)
    # with open(template_file, 'r') as file:
    #     script += file.read()

    # Generate commands for each server based on the CSV input
    for s in servers:
        # Assuming 'address' and 'port' are columns in the CSV
        address = s['address']  # e.g., 'localhost'
        port = s['port']        # e.g., '7001'
        node_id = s['id']       # e.g., '1'

        # Prepare the command to run each server
        script += f'    - reset && python run_chord_servers.py {address}:{port} --id {node_id}\n'

    # Write the generated script to a YAML file for tmuxp
    with open('launch_chord.yaml', 'w') as file:
        file.write(script)

    # # Optionally execute the tmuxp load command to start servers
    if not no_exec:
        os.system('tmuxp load -y launch_chord.yaml')


if __name__ == "__main__":
    gen_run_script('server-list.csv',False)
