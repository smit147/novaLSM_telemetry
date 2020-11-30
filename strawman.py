import copy
import time
import logging
import Utils


'''
This approach takes the highest throughput server's highest throughput range and swaps it with lowest throughput server's
lowest throughput range.
'''


def cfg_change():
    coordinator_conn = Utils.connect_with_coordinator()

    # Done already, before calling this in server.
    # Getting the initial configuration
    # Utils.read_config(Utils.args.config_filepath)



    # strawman implementation
    time.sleep(50)

    # get_one_copy_of_telemetry
    telemetry_data = Utils.get_copy_of_telemetry_data()

    per_node_throughput = [0 for _ in range(len(Utils.cur_config_state.ltcs))]
    per_range_throughput = [0 for _ in range(len(Utils.cur_config_state.ranges))]

    for node in telemetry_data["ycsb"]:
        for r in telemetry_data["ycsb"][node]:
            r_id = int(r)
            for process in telemetry_data["ycsb"][node][r]:
                num_ele = len(telemetry_data["ycsb"][node][r][process])
                avg = sum(
                    [telemetry_data["ycsb"][node][r][process][t]["throughput"] for t in range(num_ele)]) / num_ele
                per_node_throughput[Utils.cur_config_state.ranges[r_id][2]] += avg
                per_range_throughput[r_id] += avg

    # to swap, max throughput range in this node
    node_1 = per_node_throughput.index(max(per_node_throughput))
    # swap with max throughput range in this node
    node_2 = per_node_throughput.index(min(per_node_throughput))
    range_1 = Utils.get_max_throughput_range(node_1, per_range_throughput)
    range_2 = Utils.get_max_throughput_range(node_2, per_range_throughput)

    new_cfg_state = copy.deepcopy(Utils.cur_config_state)
    l1 = list(new_cfg_state.ranges[range_1])
    l2 = list(new_cfg_state.ranges[range_2])
    l1[2] = node_2
    l2[2] = node_1

    new_cfg_state.ranges[range_1] = tuple(l1)
    new_cfg_state.ranges[range_2] = tuple(l2)

    Utils.update_cfg_file(new_cfg=new_cfg_state, cfg_filepath=Utils.args.config_filepath, coordinator_conn=coordinator_conn)

    logging.info('Config change done for strawman')
