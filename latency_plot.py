import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style
import threading

p99_latency_local = {}
for i in range(4):
    p99_latency_local[i] = []
p99_latency_local_lock = threading.Lock()

style.use('fivethirtyeight')

p99_latency_fig = plt.figure()
p99_axs = [p99_latency_fig.add_subplot(2, 2, i) for i in range(1, 5)]


def p99_latency_animate(i):
    for i in range(4):
        with p99_latency_local_lock:
            if i not in p99_latency_local:
                continue
            xs = range(1, len(p99_latency_local[i])+1)
            ys = [j for j in p99_latency_local[i]]

        p99_axs[i].clear()
        p99_axs[i].plot(xs, ys)


def p99_latency_animation_thread(queue):
    while True:
        node, p99_latency = queue.get()
        with p99_latency_local_lock:
            p99_latency_local[node].append(p99_latency)


def p99_animation_process(queue):
    anim_thr = threading.Thread(target=p99_latency_animation_thread, args=(queue,))
    anim_thr.start()

    ani = animation.FuncAnimation(p99_latency_fig, p99_latency_animate, interval=1000)
    plt.show()

    anim_thr.join()