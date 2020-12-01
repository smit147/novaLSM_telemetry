import copy
import os
import socket
import threading
import logging


def connect_with_coordinator():
    socket_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_conn.connect(('127.0.0.1', args.coordinator_port))
    return socket_conn


class Config:
    def __init__(self, ltcs, stocs, start_time):
        # self.config_id = config_id
        self.ltcs = ltcs
        self.stocs = stocs
        self.start_time = start_time
        self.ranges = []

    def add_range(self, start, end, node_id, db_index):
        self.ranges.append((start, end, node_id, db_index))

    def get_str_range(self):
        output_str = ''
        for r in self.ranges:
            output_str += '{},{},{},{}\n'.format(r[0], r[1], r[2], r[3])

        return output_str

    def get_str(self, cfg_id):
        output_str = 'config-{}\n'.format(cfg_id)
        for ltc in self.ltcs:
            output_str += '{},'.format(ltc)
        output_str = output_str[:-1] + '\n'
        for stoc in self.stocs:
            output_str += '{},'.format(stoc)
        output_str = output_str[:-1] + '\n'
        output_str += '{}\n'.format(self.start_time)
        output_str += self.get_str_range()
        return output_str


def read_config(cfg_filepath):
    global cur_config_state

    cfg_file = open(cfg_filepath, 'r')
    cfg_data = cfg_file.readlines()
    ind = 0
    if 'config' in cfg_data[ind]:
        ind += 1
        ltcs = cfg_data[ind].strip().split(',')
        ind += 1
        stocs = cfg_data[ind].strip().split(',')
        ind += 1
        time = cfg_data[ind].strip()
        ind += 1
        cur_cfg = Config(ltcs, stocs, time)
        while not (ind >= len(cfg_data) or 'config' in cfg_data[ind]):
            r_info = cfg_data[ind].strip().split(',')
            cur_cfg.add_range(int(r_info[0]), int(r_info[1]), int(r_info[2]), int(r_info[3]))
            ind += 1
        cur_config_state = cur_cfg
        print(cur_config_state.get_str('~'))


def update_cfg_file(new_cfg, cfg_filepath, coordinator_conn):
    global cur_config_state

    # append new_cfg to cfg file
    cfg_file = open(cfg_filepath, 'r')
    cur_cfg_id = 0
    for line in cfg_file.readlines():
        if 'config' in line:
            cur_cfg_id = int(line.strip().split('-')[-1])

    cfg_file.close()

    cfg_file = open(cfg_filepath, 'a')
    cfg_file.write('\n'+new_cfg.get_str(cur_cfg_id + 1))
    cfg_file.close()

    # send updated cfg to servers, coordinator and client.
    for m in range(args.nmachines):
        scp_command = 'scp {} node-{}:{}'.format(cfg_filepath, m, cfg_filepath)
        logging.info(scp_command)
        os.system(scp_command)

    # notify coordinator
    coordinator_conn.sendall("change_cfg\n".encode("utf-8"))
    cur_config_state = new_cfg
    logging.info("config data sent to coordinator")


def get_max_throughput_range(node, per_range_throughput):
    max_throughput, i = max([(per_range_throughput[ind], ind) for ind in range(len(cur_config_state.ranges)) if cur_config_state.ranges[ind][2] == node])
    return (max_throughput, i)


def get_throughput_for_telemetry(local_telemetry_data, fetch_length):
    per_node_throughput = [0 for _ in range(len(cur_config_state.ltcs))]
    per_range_throughput = [0 for _ in range(len(cur_config_state.ranges))]

    for node in local_telemetry_data["ycsb"]:
        for r in local_telemetry_data["ycsb"][node]:
            r_id = int(r)
            for process in local_telemetry_data["ycsb"][node][r]:
                num_ele_to_fetch = min(len(local_telemetry_data["ycsb"][node][r][process]), fetch_length)
                num_ele = len(local_telemetry_data["ycsb"][node][r][process])

                avg = 0
                try:
                    avg = sum(
                        [local_telemetry_data["ycsb"][node][r][process][t]["throughput"] for t in range(num_ele-num_ele_to_fetch, num_ele) if
                         local_telemetry_data["ycsb"][node][r][process][t]["throughput"] > 0.0]) / num_ele
                except Exception as e:
                    logging.info("exception {}".format(e))

                per_node_throughput[cur_config_state.ranges[r_id][2]] += avg
                per_range_throughput[r_id] += avg

    return per_node_throughput, per_range_throughput


def get_p99_for_telemetry(local_telemetry_data, fetch_length):

    # tuple (i,j): for a node
    # i --> summation of latency (avg over time) of ranges
    # j --> number of ranges

    per_node_p99 = [(0, 0) for _ in range(len(cur_config_state.ltcs))]
    per_range_p99 = [(0, 0) for _ in range(len(cur_config_state.ranges))]

    for node in local_telemetry_data["ycsb"]:
        for r in local_telemetry_data["ycsb"][node]:
            r_id = int(r)
            for process in local_telemetry_data["ycsb"][node][r]:
                num_ele_to_fetch = min(len(local_telemetry_data["ycsb"][node][r][process]), fetch_length)
                num_ele = len(local_telemetry_data["ycsb"][node][r][process])

                avg = 0
                try:
                    avg = sum(
                        [local_telemetry_data["ycsb"][node][r][process][t]["read"]["p99"] for t in range(num_ele-num_ele_to_fetch, num_ele) if
                         local_telemetry_data["ycsb"][node][r][process][t]["read"]["p99"] > 0.0]) / num_ele

                except Exception as e:
                    print("exception {}".format(e))

                per_node_p99[cur_config_state.ranges[r_id][2]] = (per_node_p99[cur_config_state.ranges[r_id][2]][0]+avg,
                                                          per_node_p99[cur_config_state.ranges[r_id][2]][1]+1)

                per_range_p99[r_id] = (per_range_p99[r_id][0]+avg, per_range_p99[r_id][1]+1)

    return per_node_p99, per_range_p99


def get_copy_of_telemetry_data():
    with telemetry_data_lock:
        local_telemetry_data = copy.deepcopy(telemetry_data)

    return local_telemetry_data


args = None
cur_config_state = None
telemetry_data = {"ycsb": {}, "nova": {}}
telemetry_data_lock = threading.Lock()


def init(local_args):
    global args
    args = local_args

    # Calling read_config here. Hence, not the responsibility of cfg_change.
    read_config(args.config_filepath)



