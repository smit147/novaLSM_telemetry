import socket
import sys
import json
import threading
import multiprocessing

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import latency_plot


def plot_telemetry(telemetry):
    try:
        for r in telemetry:
            r_id = int(r)
            throughput = 0
            for p in telemetry[r]:
                if "throughput" not in telemetry[r][p]:
                    break
                throughput += float(telemetry[r][p].get("throughput", 0))

            if throughput > 0:
                throughput_queue.put((r_id, throughput))

            p99_latency = (0, 0)
            for p in telemetry[r]:
                if "read" not in telemetry[r][p] or "p99" not in telemetry[r][p]["read"]:
                    break
                p99_latency = (p99_latency[0] + float(telemetry[r][p]["read"]["p99"]), p99_latency[1] + 1)

            if p99_latency[0] + p99_latency[1] > 0:
                # print('r_id: {} latency: {}'.format(r_id, p99_latency[0]/p99_latency[1]))
                p99_latency_queue.put((r_id, p99_latency[0] / p99_latency[1]))

    except Exception as e:
        print(json.dumps(telemetry, indent=4))
        print(e)


################################################################################


throughput_local = {}
for i in range(4):
    throughput_local[i] = []
throughput_local_lock = threading.Lock()

style.use('fivethirtyeight')

throughput_fig = plt.figure()
axs = [throughput_fig.add_subplot(2, 2, i) for i in range(1, 5)]


def throughput_animate(i):
    for i in range(4):
        with throughput_local_lock:
            if i not in throughput_local:
                continue
            xs = range(1, len(throughput_local[i])+1)
            ys = [j for j in throughput_local[i]]

        axs[i].clear()
        axs[i].plot(xs, ys)


def throughtput_animation_thread(queue):
    while True:
        node, throughput = queue.get()
        with throughput_local_lock:
            throughput_local[node].append(throughput)


def throughput_animation_process(queue):
    anim_thr = threading.Thread(target=throughtput_animation_thread, args=(queue,))
    anim_thr.start()

    ani = animation.FuncAnimation(throughput_fig, throughput_animate, interval=1000)
    plt.show()

    anim_thr.join()


#######################  P99 Latency #########################################################


# p99_latency_local = {}
# for i in range(4):
#     p99_latency_local[i] = []
# p99_latency_local_lock = threading.Lock()
#
# style.use('fivethirtyeight')
#
# p99_latency_fig = plt.figure()
# p99_axs = [p99_latency_fig.add_subplot(2, 2, i) for i in range(1, 5)]
#
#
# def p99_latency_animate(i):
#     for i in range(4):
#         with p99_latency_local_lock:
#             if i not in p99_latency_local:
#                 continue
#             xs = range(1, len(p99_latency_local[i])+1)
#             ys = [j for j in p99_latency_local[i]]
#
#         p99_axs[i].clear()
#         p99_axs[i].plot(xs, ys)
#
#
# def p99_latency_animation_thread(queue):
#     while True:
#         node, p99_latency = queue.get()
#         with p99_latency_local_lock:
#             p99_latency_local[node].append(p99_latency)
#
#
# def p99_animation_process(queue):
#     anim_thr = threading.Thread(target=p99_latency_animation_thread, args=(queue,))
#     anim_thr.start()
#
#     ani = animation.FuncAnimation(p99_latency_fig, p99_latency_animate, interval=1000)
#     plt.show()
#
#     anim_thr.join()


################################################################################

temp_data = []


def fetch_data():
    global temp_data

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])

    socket_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while True:
        try:
            socket_conn.connect((server_ip, server_port))
            break
        except:
            print("connecting to server agian")
            continue

    print('connected to server')

    while True:
        data = socket_conn.recv(102400)
        if not data:
            print("no data from server")
            break

        data_str = data.decode("utf-8")

        if "end_data" not in data_str:
            temp_data.append(data_str)
        else:
            # print(data_str)
            split_data = data_str.split("end_data")
            temp_data.append(split_data[0])
            json_readable_data = ''.join(temp_data)

            # todo: handle multiple end_data by calling plot_telemetry for each completed
            temp_data = [split_data[-1]]

            try:
                telemetry = json.loads(json_readable_data)
                # print("data recieved")
                plot_telemetry(telemetry)

            except Exception as e:
                print("couldn't convert data")
                print(e)
                print(json_readable_data)
                print("\n")


fetch_thread = threading.Thread(target=fetch_data)
fetch_thread.start()

throughput_queue = multiprocessing.Queue()
p99_latency_queue = multiprocessing.Queue()

p1 = multiprocessing.Process(target=throughput_animation_process, args=(throughput_queue,))
p2 = multiprocessing.Process(target=latency_plot.p99_animation_process, args=(p99_latency_queue,))

p1.start()
p2.start()

p1.join()
p2.join()

fetch_thread.join()
