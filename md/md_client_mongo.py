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
import pymongo
from pymongo import MongoClient

parser = argparse.ArgumentParser()
parser.add_argument("--print", action="store_true", default=False,
                    help="print the logs")
args = parser.parse_args()

# Connect with md_server
endpoint="tcp://127.0.0.1:29000"
ctx = zmq.Context()
socket = ctx.socket(zmq.SUB)
socket.connect(endpoint)
socket.setsockopt_string(zmq.SUBSCRIBE, "")

# Connect with mongo database
client = MongoClient('localhost', 27017)

while True:
    msg = socket.recv()
    decodedMsg = msg.decode('utf-8')
    msgJson = json.loads(decodedMsg)
    if args.print:
        util.log_info(msgJson)

    db = client.bitfeed
    db["store_v1"].insert_one(msgJson)
