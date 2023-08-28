import subprocess
import time
from ping3 import ping


def check_server_connection():
    server_address = '10.127.4.226'
    # Ping the server to check reachability
    return ping(server_address)



def connect_to_vpn(server_address, vpn_username, vpn_password):
    vpn_command = f"/home/udit-probus/forticlientsslvpn/64bit/forticlientsslvpn_cli --server {server_address} --vpnuser {vpn_username}"

    # Run forticlientsslvpn_cli as a subprocess with pipes for stdin, stdout, and stderr
    with subprocess.Popen(vpn_command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True) as proc:
        # Provide the password when prompted
        proc.stdin.write(f"{vpn_password}\n")
        proc.stdin.flush()
        # Wait for the process to complete and capture the output
        output, errors = proc.communicate()
        # Print the output (optional, for debugging)
        print(output)
        print(errors)


server_address = "35.244.45.0"
server_port = 10443
vpn_username = "GCP-RF-Amit"
vpn_password = "V@luesGCP#2909"

while True:
    if not check_server_connection():
        print("Connection to server failed. Connecting to VPN...")
        connect_to_vpn(f"{server_address}:{server_port}", vpn_username, vpn_password)
    else:
        print("Connection to server successful. Connecting to VPN...")
        time.sleep(1)