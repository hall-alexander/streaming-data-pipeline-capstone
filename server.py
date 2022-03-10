#!/usr/bin/env python3

import sys
import socket
import selectors
import types
import pandas as pd
import numpy as np
import time

sel = selectors.DefaultSelector()


def accept_wrapper(sock):
    conn, addr = sock.accept()  # Should be ready to read
    print(f"Accepted connection from {addr}")
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)

def prepare_transmission(f):
    
    def read_large_file(file_handler, block_size=100):
        block = []
        for line in file_handler:
            block.append(line)
            if len(block) == block_size:
                yield block
                block = []
        if block:
            yield block

    def generate_data(f): # generator function
        for block in read_large_file(f):
            yield block

    gen = generate_data(f)
    return gen


def service_connection(key, mask):
    sock = key.fileobj
    data = key.data

    f = open("data.txt", "r")
    gen = prepare_transmission(f)
    while True:
        try:
            sock.sendall(bytes(",".join([x for x in next(gen)]), encoding="utf-8"))
            time.sleep(1)
        except StopIteration:
            f.close()
            sel.unregister(sock)
            sock.close()

    # if mask & selectors.EVENT_READ:
    #     recv_data = sock.recv(1024)  # Should be ready to read
    #     if recv_data:
    #         data.outb += recv_data
    #     else:
    #         print(f"Closing connection to {data.addr}")
    #         sel.unregister(sock)
    #         sock.close()
    # if mask & selectors.EVENT_WRITE:
    #     if data.outb:
    #         print(f"Echoing {data.outb!r} to {data.addr}")
    #         sent = sock.send(data.outb)  # Should be ready to write
    #         data.outb = data.outb[sent:]


if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} <host> <port>")
    sys.exit(1)

host, port = sys.argv[1], int(sys.argv[2])
lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
lsock.bind((host, port))
lsock.listen()
print(f"Listening on {(host, port)}")
lsock.setblocking(False)
sel.register(lsock, selectors.EVENT_READ, data=None)

try:
    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj)
            else:
                service_connection(key, mask)
except KeyboardInterrupt:
    print("Caught keyboard interrupt, exiting")
    sys.exit()
finally:
    sel.close()