import socket
import sys
import json
import threading
import multiprocessing

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style

# latency_fig = plt.figure()

throughput_global = {}
for i in range(4):
    throughput_global[i] = []


def plot_telemetry(telemetry):
    global count
    try:
        for r in telemetry:
            should_brk = False
            throughput = 0
            for p in telemetry[r]:
                if "throughput" not in telemetry[r][p]:
                    should_brk = True
                    break
                throughput += float(telemetry[r][p].get("throughput", 0))

            if should_brk:
                break

            throughput_global[int(r)].append(throughput)
            # print('data putting in queue {} {}'.format(r, throughput))
            queue.put((int(r), throughput))

    except Exception as e:
        print(json.dumps(telemetry, indent=4))
        print(e)


throughput_local = {}
for i in range(4):
    throughput_local[i] = []
throughput_local_lock = threading.Lock()

style.use('fivethirtyeight')

fig = plt.figure()
axs = [fig.add_subplot(2,2,i) for i in range(1,5)]


def animate(i):
    for i in range(4):
        with throughput_local_lock:
            if i not in throughput_local:
                continue
            xs = range(1, len(throughput_local[i])+1)
            ys = [j for j in throughput_local[i]]

        axs[i].clear()
        axs[i].plot(xs, ys)


def animation_thread(queue):
    # print("thread started")
    while True:
        node, throughput = queue.get()
        # print(node, throughput)
        with throughput_local_lock:
            throughput_local[node].append(throughput)

def animation_process(queue):
    # print("animation process started")
    anim_thr = threading.Thread(target=animation_thread, args=(queue, ))
    anim_thr.start()

    ani = animation.FuncAnimation(fig, animate, interval=1000)
    plt.show()

    anim_thr.join()


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

queue = multiprocessing.Queue()
p1 = multiprocessing.Process(target=animation_process, args=(queue, ))
p1.start()
p1.join()
fetch_thread.join()
