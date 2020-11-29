import copy
import json
import os
import socket
import sys
import time
import threading
import logging
import argparse


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
        output_str += '{}\n'.format(self.start_time.strip())
        output_str += self.get_str_range()
        return output_str


cur_config_state = None


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


def get_max_throughput_range(node, per_range_throughput):
    _, i = max([(per_range_throughput[ind], ind) for ind in range(len(cur_config_state.ranges)) if
                cur_config_state.ranges[ind][2] == node])
    return i


telemetry_data = json.load(open('telemetry_data.json', 'r'))

read_config('C:\\Users\\dhrum\\CLionProjects\\dynamic_range_partitioning\\config\\nova-tutorial-config')

# min_throughput = float.inf
per_node_throughput = [0 for _ in range(len(cur_config_state.ltcs))]
per_range_throughput = [0 for _ in range(len(cur_config_state.ranges))]

for node in telemetry_data["ycsb"]:
    for r in telemetry_data["ycsb"][node]:
        r_id = int(r)
        for process in telemetry_data["ycsb"][node][r]:
            num_ele = len(telemetry_data["ycsb"][node][r][process])
            avg = sum([telemetry_data["ycsb"][node][r][process][t]["throughput"] for t in range(num_ele)]) / num_ele
            per_node_throughput[cur_config_state.ranges[r_id][2]] += avg
            per_range_throughput[r_id] += avg

# to swap, max throughput range in this node
node_1 = per_node_throughput.index(max(per_node_throughput))
# swap with max throughput range in this node
node_2 = per_node_throughput.index(min(per_node_throughput))
range_1 = get_max_throughput_range(node_1, per_range_throughput)
range_2 = get_max_throughput_range(node_2, per_range_throughput)

new_cfg_state = copy.deepcopy(cur_config_state)
l1 = list(new_cfg_state.ranges[range_1])
l2 = list(new_cfg_state.ranges[range_2])
l1[2] = node_2
l2[2] = node_1

new_cfg_state.ranges[range_1] = tuple(l1)
new_cfg_state.ranges[range_2] = tuple(l2)

print(new_cfg_state.get_str('~'))