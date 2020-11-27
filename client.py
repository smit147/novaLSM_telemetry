import socket
import time
import sys

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 65432        # The port used by the server

id = sys.argv[1]

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    for i in range(10):
        data = 'Hello world ' + id + ' ' + str(i)
        data = bytes(data, 'utf-8')
        s.sendall(data)
        data = s.recv(1024)

        print('Received', repr(data))
        time.sleep(1)