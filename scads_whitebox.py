import copy
import time
import logging
import Utils


'''
[ADD DESCRIPTION]
'''

# info: threshold could be changed here. Initially set to 0.008
def is_overloaded_p99(p99):
    p99_threshold = 80000
    if p99 > p99_threshold:
        return True
    return False


def is_underloaded_throughput(throughput):
    throughput_threshold = 45000
    if throughput < throughput_threshold:
        return True
    return False

def is_overloaded_throughput(throughput):
    throughput_threshold = 50000
    if throughput < throughput_threshold:
        return True
    return False


def cfg_change():
    coordinator_conn = Utils.connect_with_coordinator()

    # Done already, before calling this in server.
    # Getting the initial configuration
    # Utils.read_config(Utils.args.config_filepath)

    # scads- white box implementation
    sleep_time_in_sec = 50
    while True:
        time.sleep(sleep_time_in_sec)

        # get_one_copy_of_telemetry
        telemetry_data = Utils.get_copy_of_telemetry_data()

        per_node_throughput, per_range_throughput = Utils.get_throughput_for_telemetry(telemetry_data, sleep_time_in_sec-5)
        per_node_p99, per_range_p99 = Utils.get_p99_for_telemetry(telemetry_data, sleep_time_in_sec-5)

        overloaded_nodes = [ind for ind in range(len(per_node_p99)) if is_overloaded_p99(per_node_p99[ind][0]/per_node_p99[ind][1])]
        underloaded_nodes = [ind for ind in range(len(per_node_throughput)) if is_underloaded_throughput(per_node_throughput[ind])]

        new_cfg_state = copy.deepcopy(Utils.cur_config_state)

        updated_not_underloaded_nodes = []

        # looping over all overloaded servers.
        for over_node_ind in overloaded_nodes:
            max_throughput_range = Utils.get_max_throughput_range(per_node_throughput[over_node_ind], per_range_throughput)
            for under_node_ind in underloaded_nodes:
                if under_node_ind in updated_not_underloaded_nodes:
                    continue
                if not is_overloaded_throughput(max_throughput_range[0]+per_node_throughput[under_node_ind]):
                    # Pushing this change to the config.
                    temp_list = list(new_cfg_state.range[max_throughput_range[1]])
                    temp_list[2] = under_node_ind
                    new_cfg_state.ranges[max_throughput_range[1]] = tuple(temp_list)

                    # Updating cur throughput for the nodes.
                    # note: overloaded server stays overloaded even after change.
                    per_node_throughput[under_node_ind] += max_throughput_range[0]
                    if not is_underloaded_throughput(per_node_throughput[under_node_ind]):
                        updated_not_underloaded_nodes.append(under_node_ind)
                    break

        Utils.update_cfg_file(new_cfg=new_cfg_state, cfg_filepath=Utils.args.config_filepath, coordinator_conn=coordinator_conn)

        logging.info('Config change done for scads white box')
