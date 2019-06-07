import zmq
import traceback
import json
import argparse
import sys
import os
bd = os.path.dirname(os.path.abspath(sys.argv[0]))
for p in [bd+"/../util",]:
    sys.path.append(p)
import util

parser = argparse.ArgumentParser()
parser.add_argument("--print", action="store_true", default=False,
                    help="print the logs")
args = parser.parse_args()

# Connect with IPC fed by exchange_md.py
ctx = zmq.Context()
pullEndpoint="ipc:///home/ubuntu/tmp/.md_server_private_zmq_address"
pullSocket = ctx.socket(zmq.PULL)
pullSocket.bind(pullEndpoint)
pullSocket.setsockopt(zmq.RCVHWM, 100000)

# Expose 29000 port to database / application clients
pubEndpoint="tcp://127.0.0.1:29000"
pubSocket = ctx.socket(zmq.PUB)
pubSocket.bind(pubEndpoint)
pubSocket.setsockopt(zmq.SNDHWM, 100000)

while True:
    try:
        msg = pullSocket.recv()
        decodedMsg = msg.decode('utf-8')
        msgJson = json.loads(decodedMsg)
        if args.print:
            util.log_info(msgJson)
        pubSocket.send_string(decodedMsg)

    except KeyboardInterrupt as e:
        raise
    except Exception as e:
        traceback.print_exc()
        util.log_warning("except caught")
