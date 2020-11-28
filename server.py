import json
import os
import socket
import sys
import time
import threading
import logging
import argparse
# port = 65432        # Port to listen on (non-privileged ports are > 1023)

# sys.stdout = open(sys.argv[7], 'w')

connections = []
nova_machines = []
ycsb_machines = []
telemetry_data_lock = threading.Lock()

telemetry_data = {"ycsb": {}, "nova": {}}


def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(
        description="Run placement mapper."
    )
    parser.add_argument("--nmachines", type=int)
    parser.add_argument("--nclients", type=int)
    parser.add_argument("--ranges", type=int)
    parser.add_argument("--max_processes", type=int)
    parser.add_argument("--result_dir", type=str)
    parser.add_argument("--ip", type=str)
    parser.add_argument("--port", type=int)
    parser.add_argument("--telemetry_filepath", type=str)
    parser.add_argument("--config_filepath", type=str)
    parser.add_argument("--should_wait", action='store_true')
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args(args=args)
    return args


# run parser on all machines now
def get_nova_command(node, dir, ip, port):
    nova_cmd = "\"python -u telemetry/nova_parser.py {} {} {} {} >& {}/parser-out\""\
        .format(node, dir, ip, port, dir)
    return nova_cmd


def get_ycsb_command(node, dir, ip, port, ranges, max_processes):
    ycsb_cmd = "\"python -u telemetry/ycsb_parser.py {} {} {} {} {} {} >& {}/parser-out\""\
        .format(node, dir, ip, port, ranges, max_processes, dir)
    return ycsb_cmd


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
                with telemetry_data_lock:
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
        logging.debug("Error parsing ycsb: ", e)
        logging.debug(json.dumps(telemetry, indent=4))


def append_nova(node, telemetry):
    try:
        with telemetry_data_lock:
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
        logging.debug('Error parsing nova: ', e)
        logging.debug(json.dumps(telemetry, indent=4))


def append_telemetry(telemetry):
    try:
        node = int(telemetry["server_id"])
        if node >= nmachines - nclients:
            append_ycsb(node, telemetry)
        else:
            append_nova(node, telemetry)
    except Exception as e:
        logging.debug("couldn't find server id in telemetry: ", e)
        logging.debug(json.dumps(telemetry, indent=4))
        return

    return


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


def update_cfg_file(new_cfg, cfg_filepath):
    # append new_cfg to cfg file
    cfg_file = open(cfg_filepath, 'w')
    cur_cfg_id = 0
    for line in cfg_file:
        if 'config' in line:
            cur_cfg_id = line.strip().split('-')[-1]

    cfg_file.write(new_cfg.get_str(cur_cfg_id+1))
    # send updated cfg to servers, coordinator and client.
    for m in range(nmachines):
        os.system('scp {} node-{}:{}'.format(cfg_filepath, m, cfg_filepath))

    # todo: notify coordinator


def cfg_change():
    time.sleep(20)
    new_cfg = Config([0, 1], [2, 3], 50)
    new_cfg.add_range(0, 250000, 0, 0)
    new_cfg.add_range(250000, 500000, 1, 1)
    new_cfg.add_range(500000, 625000, 0, 2)
    new_cfg.add_range(625000, 1000000, 1, 3)
    update_cfg_file(new_cfg=new_cfg, cfg_filepath=args.config_filepath)


def data_collection():
    while True:
        counter = 0
        brk = False
        for i, conn in enumerate(connections):
            data = conn.recv(102400)
            if not data:
                logging.info("no data from {}".format(i))
                brk = True
                break

            conn.sendall(b'1')

            data_str = data.decode("utf-8")

            try:
                telemetry = json.loads(data_str)
                append_telemetry(telemetry)

            except Exception as e:
                logging.debug("couldn't convert the input string: ", e)
                logging.debug('string: ', data_str)
        counter += 1
        if brk:
            break

    telemetry_file = open(telemetry_filepath, 'w')
    json.dump(telemetry_data, telemetry_file)
    print(json.dumps(telemetry_data, indent=4))


def main():
    # start server
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.bind(('', port))
    s.listen(5)

    # start clients
    # todo: decide and do change this into another thread.
    for i in range(nmachines - nclients):
        nova_cmd = get_nova_command(i, result_dir, ip, port)
        client_host = "node-{}".format(i)
        system_cmd = "ssh -oStrictHostKeyChecking=no {} {} &".format(client_host, nova_cmd)
        logging.info(system_cmd)
        os.system(system_cmd)
        time.sleep(1)

    for i in range(nmachines - nclients, nmachines):
        ycsb_cmd = get_ycsb_command(i, result_dir, ip, port, ranges, max_processes)
        system_cmd = "ssh -oStrictHostKeyChecking=no node-{} {} &".format(i, ycsb_cmd)
        logging.info(system_cmd)
        os.system(system_cmd)
        time.sleep(1)

    if should_wait:
        wait_threshold = nmachines + 1
    else:
        wait_threshold = nmachines

    # wait for connection
    while True:
        conn, addr = s.accept()
        logging.info("connected to {}".format(addr))
        connections.append(conn)
        if len(connections) == wait_threshold:
            break

    logging.info('Creating thread for configuration change')
    cfg_change_thread = threading.Thread(target=cfg_change)
    cfg_change_thread.start()

    logging.info('starting data collection thread')
    server_thread = threading.Thread(target=data_collection)
    server_thread.start()

    cfg_change_thread.join()
    server_thread.join()


if __name__ == '__main__':
    args = parse_args()

    nmachines = args.nmachines
    nclients = args.nclients
    ranges = args.ranges
    max_processes = args.max_processes
    result_dir = args.result_dir
    ip = args.ip
    port = args.port
    telemetry_filepath = args.telemetry_filepath
    should_wait = args.should_wait

    log_format = "%(asctime)s: %(message)s"
    if args.verbose:
        logging.basicConfig(format=log_format, level=logging.DEBUG, datefmt="%H:%M:%S")
    else:
        logging.basicConfig(format=log_format, level=logging.INFO, datefmt="%H:%M:%S")
    main()

