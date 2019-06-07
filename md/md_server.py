import zmq
import traceback
import json
import sys
import os
bd = os.path.dirname(os.path.abspath(sys.argv[0]))
for p in [bd+"/../util",]:
    sys.path.append(p)
import util

ctx = zmq.Context()
pullEndpoint="ipc:///home/ubuntu/tmp/.md_server_private_zmq_address"
pullSocket = ctx.socket(zmq.PULL)
pullSocket.bind(pullEndpoint)
pullSocket.setsockopt(zmq.RCVHWM, 100000)

while True:
    try:
        msg = pullSocket.recv()
        decodedMsg = msg.decode('utf-8')
        msgJson = json.loads(decodedMsg)
        util.log_info(msgJson)

    except KeyboardInterrupt as e:
        raise
    except Exception as e:
        traceback.print_exc()
        util.log_warning("except caught")
