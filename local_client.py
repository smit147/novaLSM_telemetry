import socket
import sys
import json
import threading

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style

style.use('fivethirtyeight')

fig = plt.figure()
axs = [fig.add_subplot(2,2,i) for i in range(1,5)]

latency_fig = plt.figure()

throughput_global = {}
for i in range(4):
    throughput_global[i] = []

throughput_global_lock = threading.Lock()

def animate(i):
    for i in range(4):
        with throughput_global_lock:
            xs = range(1, len(throughput_global[i])+1)
            ys = [j for j in throughput_global[i]]

        axs[i].clear()
        axs[i].plot(xs, ys)

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

            with throughput_global_lock:
                throughput_global[int(r)].append(throughput)

    except Exception as e:
        print(json.dumps(telemetry, indent=4))
        print(e)


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

temp_data = []

def animation_thr():
    ani = animation.FuncAnimation(fig, animate, interval=1000)
    plt.show()

# animation_thr()
# print("running anim")

def fetch_data():

    global temp_data

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
                plot_telemetry(telemetry)

            except Exception as e:
                print("couldn't convert data")
                print(e)
                print(json_readable_data)
                print("\n")


fetch_thread = threading.Thread(target=fetch_data)
fetch_thread.start()

animation_thr()
fetch_thread.join()
