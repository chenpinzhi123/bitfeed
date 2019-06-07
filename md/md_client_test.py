import zmq
import traceback
import json
import sys
import os
bd = os.path.dirname(os.path.abspath(sys.argv[0]))
for p in [bd+"/../util",]:
    sys.path.append(p)
import util

endpoint="tcp://127.0.0.1:29000"
ctx = zmq.Context()
socket = ctx.socket(zmq.SUB)
socket.connect(endpoint)
socket.setsockopt_string(zmq.SUBSCRIBE, "")

while True:
    msg = socket.recv()
    util.log_info(msg.decode('utf-8'))
