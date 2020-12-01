import json
import socket
import os
import sys
import time

def convert_float(val):
    try:
        return float(val)
    except:
        return 0

def parse_performance(file):
    local_performance_info = {}
    prev_copy = {}

    lines = file.readlines()
    if debug:
        print("lines read: ")
        print(lines)
    for line in lines:
        if "current ops/sec" in line:
            try:
                ems = line.split(" ")
                data_time = ems[1][:8]
                local_performance_info["data_time"] = data_time

                ems = line.split(";")
                latencies = ems[2].split(",")
                thp = ems[1]
                thp = thp.replace("current ops/sec", "")
                thp = convert_float(thp.replace(" ", ""))

                read_perf = {}
                write_perf = {}

                for i in range(len(latencies)):
                    if "READ" in latencies[i]:
                        read_perf["Max"] = convert_float(latencies[i + 1].split("=")[1])
                        read_perf["Min"] = convert_float(latencies[i + 2].split("=")[1])
                        read_perf["Avg"] = convert_float(latencies[i + 3].split("=")[1])
                        read_perf["p95"] = convert_float(latencies[i + 4].split("=")[1])
                        read_perf["p99"] = convert_float(latencies[i + 5].split("=")[1])
                        read_perf["p999"] = convert_float(latencies[i + 6].split("=")[1])
                        read_perf["p9999"] = convert_float(latencies[i + 7].split("=")[1])
                        read_perf["violation count"] = convert_float(latencies[i + 8].split("=")[1])
                        read_perf["violation fraction"] = convert_float(latencies[i + 9].split("]")[0].split("=")[1])

                    if "UPDATE" in latencies[i]:
                        write_perf["Max"] = convert_float(latencies[i + 1].split("=")[1])
                        write_perf["Min"] = convert_float(latencies[i + 2].split("=")[1])
                        write_perf["Avg"] = convert_float(latencies[i + 3].split("=")[1])
                        write_perf["p95"] = convert_float(latencies[i + 4].split("=")[1])
                        write_perf["p99"] = convert_float(latencies[i + 5].split("=")[1])
                        write_perf["p999"] = convert_float(latencies[i + 6].split("=")[1])
                        write_perf["p9999"] = convert_float(latencies[i + 7].split("=")[1])
                        write_perf["violation count"] = convert_float(latencies[i + 8].split("=")[1])
                        write_perf["violation fraction"] = convert_float(latencies[i + 9].split("]")[0].split("=")[1])

                local_performance_info["throughput"] = thp
                local_performance_info["read"] = read_perf
                local_performance_info["write"] = write_perf
            except:
                print(line)
                break
            prev_copy = local_performance_info

    return prev_copy

    # while True:
    #     line = file.readline()
    #     if "current ops/sec" in line:
    #         ems = line.split(";")
    #         latencies = ems[2].split(",")
    #         thp = ems[1]
    #         thp = thp.replace("current ops/sec", "")
    #         thp = convert_float(thp.replace(" ", ""))
    #
    #         read_perf = {}
    #         write_perf = {}
    #
    #         for i in range(len(latencies)):
    #             if "READ" in latencies[i]:
    #                 read_perf["Max"] = convert_float(latencies[i + 1].split("=")[1])
    #                 read_perf["Min"] = convert_float(latencies[i + 2].split("=")[1])
    #                 read_perf["Avg"] = convert_float(latencies[i + 3].split("=")[1])
    #                 read_perf["p95"] = convert_float(latencies[i + 4].split("=")[1])
    #                 read_perf["p99"] = convert_float(latencies[i + 5].split("=")[1])
    #                 read_perf["p999"] = convert_float(latencies[i + 6].split("=")[1])
    #                 read_perf["p9999"] = convert_float(latencies[i + 7].split("=")[1])
    #                 read_perf["violation count"] = convert_float(latencies[i + 8].split("=")[1])
    #                 read_perf["violation fraction"] = convert_float(latencies[i + 9].split("]")[0].split("=")[1])
    #
    #             if "UPDATE" in latencies[i]:
    #                 write_perf["Max"] = convert_float(latencies[i + 1].split("=")[1])
    #                 write_perf["Min"] = convert_float(latencies[i + 2].split("=")[1])
    #                 write_perf["Avg"] = convert_float(latencies[i + 3].split("=")[1])
    #                 write_perf["p95"] = convert_float(latencies[i + 4].split("=")[1])
    #                 write_perf["p99"] = convert_float(latencies[i + 5].split("=")[1])
    #                 write_perf["p999"] = convert_float(latencies[i + 6].split("=")[1])
    #                 write_perf["p9999"] = convert_float(latencies[i + 7].split("=")[1])
    #                 write_perf["violation count"] = convert_float(latencies[i + 8].split("=")[1])
    #                 write_perf["violation fraction"] = convert_float(latencies[i + 9].split("]")[0].split("=")[1])
    #
    #         local_performance_info["throughput"] = thp
    #         local_performance_info["read"] = read_perf
    #         local_performance_info["write"] = write_perf
    #         break
    # return local_performance_info


def send_data(performance_info):
    global count
    if "throughput" in performance_info[0][0]:
        print("node 0 throughput -> {}".format(performance_info[0][0]["throughput"]))
    else:
        print(json.dumps(performance_info, indent=4))

    print("sending data {}".format(count))
    count += 1
    if len(performance_info) == 0:
        print("no data to send")
        print("\n")
        return
    combined_info = {
        "server_id": node,
        "performance_info": performance_info
    }
    combined_str = json.dumps(combined_info)
    data = combined_str.encode('utf-8')
    print(sys.getsizeof(data))
    print(len(data))
    socket_conn.sendall(data)
    socket_conn.recv(128)
    print("data sent")
    print("\n")


node = sys.argv[1]
result_dir = sys.argv[2]
server_ip = sys.argv[3]
server_port = int(sys.argv[4])
ranges = int(sys.argv[5])
max_processes = int(sys.argv[6])
count = 0

## create connection to server now
socket_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket_conn.connect((server_ip, server_port))
debug = False

for range_id in range(ranges):
    for process_id in range(1):
        filename = os.path.join(result_dir, "client-node-{}-{}-{}-out".format(node, range_id, process_id))
        while not os.path.exists(filename):
            pass

files = {}

for range_id in range(ranges):
    for process_id in range(max_processes):
        try:
            files["{}-{}".format(range_id,process_id)] = open(os.path.join(result_dir, "client-node-{}-{}-{}-out".format(node, range_id, process_id)))
        except:
            continue

while True:
    performance_info = {}
    for range_id in range(ranges):
        performance_info[range_id] = {}
        for process_id in range(max_processes):
            if "{}-{}".format(range_id, process_id) in files:
                if debug:
                    print("parsing data for range: {} process {}".format(range_id, process_id))
                local_perf = parse_performance(files["{}-{}".format(range_id, process_id)])
                performance_info[range_id][process_id] = local_perf

    send_data(performance_info)
    time.sleep(.5)
