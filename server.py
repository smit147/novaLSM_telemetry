import json
import os
import socket
import sys
import time

# port = 65432        # Port to listen on (non-privileged ports are > 1023)



nmachines = int(sys.argv[1])
nclients = int(sys.argv[2])
ranges = int(sys.argv[3])
max_processes = int(sys.argv[4])
result_dir = sys.argv[5]
ip = sys.argv[6]
port = int(sys.argv[7])

# sys.stdout = open(sys.argv[7], 'w')

assert nclients <= nmachines

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.bind(('', port))
s.listen(5)

connections = []
nova_machines = []
ycsb_machines = []

# run parser on all machines now
def get_nova_command(node, dir, ip, port):
    nova_cmd = "\"python -u telemetry/nova_parser.py {} {} {} {} >& {}/parser-out\""\
        .format(node, dir, ip, port, dir)
    return nova_cmd


def get_ycsb_command(node, dir, ip, port, ranges, max_processes):
    ycsb_cmd = "\"python -u telemetry/ycsb_parser.py {} {} {} {} {} {} >& {}/parser-out\""\
        .format(node, dir, ip, port, ranges, max_processes, dir)
    return ycsb_cmd


for i in range(nmachines-nclients):
    nova_cmd = get_nova_command(i, result_dir, ip, port)
    system_cmd = "ssh -oStrictHostKeyChecking=no node-{} {} &".format(i, nova_cmd)
    print(system_cmd)
    os.system(system_cmd)
    time.sleep(1)

for i in range(nmachines - nclients, nmachines):
    ycsb_cmd = get_ycsb_command(i, result_dir, ip, port, ranges, max_processes)
    system_cmd = "ssh -oStrictHostKeyChecking=no node-{} {} &".format(i, ycsb_cmd)
    print(system_cmd)
    os.system(system_cmd)
    time.sleep(1)


def append_telemetry(telemetry):
    pass

# wait for connection
while True:
    conn, addr = s.accept()
    print("connected to {}".format(addr))
    connections.append(conn)
    if len(connections) == nmachines:
        break

telemetry_data = {}
while True:
    counter = 0
    brk = False
    for i, conn in enumerate(connections):
        data = conn.recv(1024)
        # print("data from ", str(i), " --> ", repr(data))
        if not data:
            print("no data from {}".format(i))
            brk = True
            break

        conn.sendall(b'1')

        # data_str = repr(data)
        data_str = data.decode("utf-8")

        # print(data_str)
        # print()
        try:
            telemetry = json.loads(data_str)
            append_telemetry(telemetry)

        except:
            print("couldn't convert the input string")
    counter += 1
    if brk:
        break