import copy
import time
import logging
import Utils


'''
[ADD DESCRIPTION]
'''
underloaded_workloadc_thorughput_threshold = 40000
overloaded_workloadc_thorughput_threshold = 45000

underloaded_workloadb_thorughput_threshold = 45000
overloaded_workloadb_thorughput_threshold = 50000

# info: threshold could be changed here. Initially set to 0.008
def is_overloaded_p99(p99):
    p99_threshold = 80000
    if p99 > p99_threshold:
        return True
    return False


def is_underloaded_throughput(throughput):
    throughput_threshold = underloaded_workloadc_thorughput_threshold
    if throughput < throughput_threshold:
        return True
    return False


def is_overloaded_throughput(throughput):
    throughput_threshold = overloaded_workloadc_thorughput_threshold
    if throughput > throughput_threshold:
        return True
    return False


def sort_by_value_decreasing(li, func):
    li = copy.deepcopy(li)
    new_li = [(ind, li[ind]) for ind in range(len(li))]
    new_li.sort(key=func, reverse=True)
    return new_li


def placement_reorganisation(telemetry_data, sleep_time_in_sec):
    per_node_throughput, per_range_throughput = Utils.get_throughput_for_telemetry(telemetry_data,
                                                                                   sleep_time_in_sec - 5)
    per_node_p99, per_range_p99 = Utils.get_p99_for_telemetry(telemetry_data, sleep_time_in_sec - 5)
    logging.info(str(per_range_throughput))
    logging.info(str(per_range_p99))

    # Segregating node and its value, so that we can sort and iterate in decreasing value.
    node_p99 = sort_by_value_decreasing(per_node_p99, lambda x: x[1][0] / x[1][1])
    node_throughput = sort_by_value_decreasing(per_node_throughput, lambda x: x[1])
    # range_p99 = sort_by_value_decreasing(per_range_p99, lambda x: x[1][0] / x[1][1])
    range_throughput = sort_by_value_decreasing(per_range_throughput, lambda x: x[1])

    overloaded_nodes = [val[0] for val in node_p99 if val[1][1] > 0 and is_overloaded_p99(val[1][0] / val[1][1])]
    underloaded_nodes = [val[0] for val in node_throughput if is_underloaded_throughput(val[1])]

    logging.info(str(overloaded_nodes))
    logging.info(str(underloaded_nodes))

    new_cfg_state = copy.deepcopy(Utils.cur_config_state)

    updated_not_underloaded_nodes = []

    # looping over all overloaded servers.
    for overloaded_nodes_id in overloaded_nodes:
        logging.info('over_node_id {}'.format(overloaded_nodes_id))

        overloaded_node_throughput = [val[1] for val in node_throughput if val[0] == overloaded_nodes_id]
        overloaded_node_throughput = overloaded_node_throughput[0]

        overloaded_node_avg_p99 = [val[1][0] / val[1][1] for val in node_p99 if val[0] == overloaded_nodes_id]
        overloaded_node_avg_p99 = overloaded_node_avg_p99[0]

        logging.info('over_node\'s throughput {} p99 {}'.format(overloaded_node_throughput, overloaded_node_avg_p99))

        overloaded_node_handled = False

        for overloaded_range in range_throughput:
            if Utils.cur_config_state.ranges[overloaded_range[0]][2] != overloaded_nodes_id:
                continue

            logging.info('current max throughput range {} {}'.format(overloaded_range[0], overloaded_range[1]))

            for underloaded_nodes_id in underloaded_nodes:
                if underloaded_nodes_id == overloaded_nodes_id:
                    continue
                logging.info('under_node_id {}'.format(underloaded_nodes_id))

                underloaded_node_throughput = [val[1] for val in node_throughput if val[0] == underloaded_nodes_id]
                underloaded_node_throughput = underloaded_node_throughput[0]

                underloaded_node_avg_p99 = [val[1][0] / val[1][1] for val in node_p99 if val[0] == underloaded_nodes_id]
                underloaded_node_avg_p99 = underloaded_node_avg_p99[0]

                logging.info(
                    'under_node\'s throughput {} p99 {}'.format(underloaded_node_throughput, underloaded_node_avg_p99))

                if underloaded_nodes_id in updated_not_underloaded_nodes:
                    continue
                if not is_overloaded_throughput(overloaded_range[1] + underloaded_node_throughput):
                    logging.info('CHANGING CONFIG: Migrating Range-{} of node-{} to node-{}'.format(overloaded_range[0],
                                                                                                    overloaded_nodes_id,
                                                                                                    underloaded_nodes_id))
                    # Pushing this change to the config.
                    temp_list = list(new_cfg_state.ranges[overloaded_range[0]])
                    temp_list[2] = underloaded_nodes_id
                    new_cfg_state.ranges[overloaded_range[0]] = tuple(temp_list)

                    # Updating cur throughput for the nodes.
                    # note: overloaded server stays overloaded even after change.
                    underloaded_nodes_id_index = \
                    [ind for ind in range(len(node_throughput)) if node_throughput[ind][0] == underloaded_nodes_id][0]
                    node_throughput[underloaded_nodes_id_index] = (node_throughput[underloaded_nodes_id_index][0],
                                                                   node_throughput[underloaded_nodes_id_index][1] +
                                                                   overloaded_range[1])
                    if not is_underloaded_throughput(node_throughput[underloaded_nodes_id_index][1]):
                        updated_not_underloaded_nodes.append(underloaded_nodes_id)
                    overloaded_node_handled = True
                    break

            if overloaded_node_handled:
                break
    return new_cfg_state


def cfg_change():
    coordinator_conn = Utils.connect_with_coordinator()

    # scads- white box implementation
    sleep_time_in_sec = 50
    while True:
        time.sleep(sleep_time_in_sec)

        # get_one_copy_of_telemetry
        telemetry_data = Utils.get_copy_of_telemetry_data()

        new_cfg_state = placement_reorganisation(telemetry_data, sleep_time_in_sec-5)

        Utils.update_cfg_file(new_cfg=new_cfg_state, cfg_filepath=Utils.args.config_filepath, coordinator_conn=coordinator_conn)

        logging.info('Config change done for scads white box')


def cfg_change_test():

    # scads- white box implementation
    sleep_time_in_sec = 50
    while True:
        # time.sleep(sleep_time_in_sec)

        # get_one_copy_of_telemetry
        telemetry_data = Utils.get_copy_of_telemetry_data_test()

        new_cfg_state = placement_reorganisation(telemetry_data, sleep_time_in_sec-5)

        logging.info("updated config:\n{}".format(new_cfg_state.get_str('~')))