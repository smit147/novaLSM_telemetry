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
should_wait = sys.argv[8]

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
    client_host = "node-{}".format(i)
    system_cmd = "ssh -oStrictHostKeyChecking=no {} {} &".format(client_host, nova_cmd)
    print(system_cmd)
    os.system(system_cmd)
    time.sleep(1)

for i in range(nmachines - nclients, nmachines):
    ycsb_cmd = get_ycsb_command(i, result_dir, ip, port, ranges, max_processes)
    system_cmd = "ssh -oStrictHostKeyChecking=no node-{} {} &".format(i, ycsb_cmd)
    print(system_cmd)
    os.system(system_cmd)
    time.sleep(1)


def getTime():
    return time.ctime().split()[3]


def convert_float(val):
    try:
        return float(val)
    except:
        return 0


def convert_dict_float(str_dict):
    local_dict = {}
    for k,v in str_dict.items():
        local_dict[k] = convert_float(v)

    return local_dict


def append_ycsb(node, telemetry):
    try:
        performance = telemetry["performance_info"]
        for range in performance.keys():
            for process in performance[range].keys():
                range_id = int(range)
                process_id = int(process)
                fetched_performance = performance[range][process]

                if len(fetched_performance) == 0:
                    continue
                if node not in telemetry_data["ycsb"]:
                    telemetry_data["ycsb"][node] = {}
                if range_id not in telemetry_data["ycsb"][node]:
                    telemetry_data["ycsb"][node][range_id] = {}
                if process_id not in telemetry_data["ycsb"][node][range_id]:
                    telemetry_data["ycsb"][node][range_id][process_id] = []

                local_performance = {"data_time": fetched_performance["data_time"],
                                     "fetch_time": getTime(),
                                     "throughput": float(fetched_performance["throughput"]),
                                     "read": convert_dict_float(fetched_performance["read"]),
                                     "write": convert_dict_float(fetched_performance["write"])}

                telemetry_data["ycsb"][node][range_id][process_id].append(local_performance)
    except Exception as e:
        print("Error parsing ycsb: ", e)
        print(json.dumps(telemetry, indent=4))



def append_nova(node, telemetry):
    try:
        # telemetry_data["nova"]["node_id"]["cpu_info"].append(telemetry["cpu_info"])
        telemetry_data["nova"][node] = telemetry_data["nova"].get(node, {})
        telemetry_data["nova"][node]['cpu_info'] = telemetry_data["nova"][node].get('cpu_info', [])
        telemetry_data["nova"][node]['disk_info'] = telemetry_data["nova"][node].get('disk_info', [])
        telemetry_data["nova"][node]['mem_info'] = telemetry_data["nova"][node].get('mem_info', [])
        telemetry_data["nova"][node]['rdma_info'] = telemetry_data["nova"][node].get('rdma_info', [])
        telemetry_data["nova"][node]['net_info'] = telemetry_data["nova"][node].get('net_info', [])

        if len(telemetry['cpu_info']) != 0:
            telemetry_data["nova"][node]['cpu_info'].append({'data_time': telemetry['cpu_info']['data_time'],
                                                             'fetch_time': getTime(),
                                                             'all': telemetry['cpu_info']['all']})
        if len(telemetry['disk_info']) != 0:
            telemetry_data["nova"][node]['disk_info'].append({'data_time': telemetry['disk_info']['data_time'],
                                                              'fetch_time': getTime(),
                                                              'util': telemetry['disk_info']['util']})
        # Add others if required.
    except Exception as e:
        print('Error parsing nova: ', e)
        print(json.dumps(telemetry, indent=4))


def append_telemetry(telemetry):
    try:
        node = int(telemetry["server_id"])
        if node >= nmachines - nclients:
            append_ycsb(node, telemetry)
        else:
            append_nova(node, telemetry)

    except:
        print("couldn't find server id in telemetry: ")
        print(json.dumps(telemetry, indent=4))
        return

    return

if should_wait:
    wait_threshold = nmachines + 1
else:
    wait_threshold = nmachines
# wait for connection
while True:
    conn, addr = s.accept()
    print("connected to {}".format(addr))
    connections.append(conn)
    if len(connections) == wait_threshold:
        break

telemetry_data = {}
telemetry_data["ycsb"] = {}
telemetry_data["nova"] = {}

while True:
    counter = 0
    brk = False
    for i, conn in enumerate(connections):
        data = conn.recv(102400)
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

        except Exception as e:
            print("couldn't convert the input string: ", e)
            print('string: ', data_str)
    counter += 1
    if brk:
        break

print(json.dumps(telemetry_data, indent=4))
