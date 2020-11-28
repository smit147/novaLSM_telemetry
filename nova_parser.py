import json
import socket
import os
import sys
import time


def remove_empty(array):
    narray = []
    for em in array:
        if em != '':
            narray.append(em)
    return narray

def convert_float(val):
    try:
        return float(val)
    except:
        return 0


def parse_cpu(file):
    local_cpu_info = {}
    prev_copy = {}
    lines = file.readlines()

    for line in lines:
        if "all" in line:
            try:
                t = line.split(" ")[0]
                if line.split(" ")[1] == "PM":
                    splt_t = t.split(":")
                    hours_int = int(splt_t[0])
                    if hours_int != 12:
                        hours_int += 12
                    t = "{}:{}:{}".format(str(hours_int), splt_t[1], splt_t[2])

                local_cpu_info["data_time"] = t
            except:
                print("couldn't convert time in cpu")
                print(line)
                break
            try:
                cpuarray = remove_empty(line.split(" "))
                local_cpu_info["all"] = 100.0 - convert_float(cpuarray[-1])
            except:
                print("couldn't convert cpu")
                print(line)
                break

            cur_index = lines.index(line) + 1
            for j in range(cur_index, cur_index + num_cores):
                try:
                    l = lines[j]
                    cpuarray = remove_empty(l.split(" "))
                    local_cpu_info["{}".format(j)] = 100.0 - convert_float(cpuarray[-1])
                except:
                    print("couldn't convert cpu")
                    print(line, j)
                    break

        prev_copy = local_cpu_info
    return prev_copy

def parse_rdma(file):
    local_rdma_info = {}
    return local_rdma_info

def parse_net(file):
    local_net_info = {}
    return local_net_info

def parse_disk(file):
    local_disk_info = {}
    prev_copy = {}
    lines = file.readlines()
    for line in lines:
        if "dev8-0" in line:
            try:
                t = line.split(" ")[0]
                if line.split(" ")[1] == "PM":
                    splt_t = t.split(":")
                    hours_int = int(splt_t[0])
                    if hours_int != 12:
                        hours_int += 12
                    t = "{}:{}:{}".format(str(hours_int), splt_t[1], splt_t[2])

                local_disk_info["data_time"] = t

                diskarray = remove_empty(line.split(" "))
                local_disk_info["util"] = ((convert_float(diskarray[3]),
                              (convert_float(diskarray[5])) / 2 / 1024,
                              convert_float(diskarray[6]) / 2,
                              convert_float(diskarray[7]),
                              convert_float(diskarray[4]),
                              convert_float(diskarray[8]),
                              convert_float(diskarray[9]),
                              convert_float(diskarray[10])
                              ))
            except:
                print(line)
                break

        prev_copy = local_disk_info

    return prev_copy

def parse_mem(file):
    local_mem_info = {}
    return local_mem_info

def send_data(cpu_info, rdma_info, disk_info, mem_info, net_info):
    global count
    print("sending data {}".format(count))
    count += 1
    if len(cpu_info) == 0 and len(rdma_info) == 0 and len(disk_info) == 0 and len(mem_info) == 0 and len(net_info) == 0:
        print("no data to send")
        print("\n")
        return
    combined_info = {
        "server_id": node,
        "cpu_info": cpu_info,
        "rdma_info": rdma_info,
        "disk_info": disk_info,
        "mem_info": mem_info,
        "net_info": net_info
    }
    combined_str = json.dumps(combined_info)
    data = combined_str.encode('utf-8')
    socket_conn.sendall(data)
    socket_conn.recv(128)
    print(sys.getsizeof(data))
    print(len(data))
    print("data sent")
    print("\n")

node = sys.argv[1]
result_dir = sys.argv[2]
server_ip = sys.argv[3]
server_port = int(sys.argv[4])

num_cores  = 32
count = 0

## create connection to server now
socket_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket_conn.connect((server_ip, server_port))

cpu_filename = os.path.join(result_dir, 'node-'+node+'-cpu.txt')
rdma_filename = os.path.join(result_dir, 'node-'+node+'-coll.txt')
disk_filename = os.path.join(result_dir, 'node-'+node+'-disk.txt')
mem_filename = os.path.join(result_dir, 'node-'+node+'-mem.txt')
net_filename = os.path.join(result_dir, 'node-'+node+'-net.txt')

# print(cpu_filename)

while True:
    while not (os.path.exists(cpu_filename) and os.path.exists(rdma_filename)
               and os.path.exists(rdma_filename) and os.path.exists(mem_filename) and os.path.exists(net_filename)):
        pass

    try:
        cpu_file = open(cpu_filename)
        rdma_file = open(rdma_filename)
        disk_file = open(disk_filename)
        mem_file = open(mem_filename)
        net_file = open(net_filename)
        break

    except:
        print("trying again")
        continue

last_disk_pointer = 0

while True:
    cpu_info = parse_cpu(cpu_file)
    rdma_info = parse_rdma(rdma_file)
    disk_info = parse_disk(disk_file)
    mem_info = parse_mem(mem_file)
    net_info = parse_net(net_file)

    send_data(cpu_info, rdma_info, disk_info, mem_info, net_info)
    time.sleep(0.5)