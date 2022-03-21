#!/usr/bin/env python3

import socket
import json
import time
from kafka import KafkaProducer

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 1234  # The port used by the server

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    while True:
        data = s.recv(900)
        data = data.decode(encoding="utf-8")
        data = data.strip()
        data = json.loads(data)
        producer.send('position_history', value=data)