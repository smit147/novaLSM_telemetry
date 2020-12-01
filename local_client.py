import os
import sys

ip = sys.argv[1]
port = sys.argv[2]

os.system("python graphs/throughput.py {} {} > graphs/throughput_out &".format(ip, port))
os.system("python graphs/latency.py {} {} > graphs/latency_out &".format(ip, port))