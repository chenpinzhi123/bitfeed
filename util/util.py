import zmq
import time
import datetime

def log_info(*args, **kwargs):
    ts = time.time()
    print(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'), end=" ")
    print("### INFO ###", end=" ")
    print(*args, **kwargs)

def log_warning(*args, **kwargs):
    ts = time.time()
    print(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'), end=" ")
    print("### WARNING ###", end=" ")
    print(*args, **kwargs)

def log_error(*args, **kwargs):
    ts = time.time()
    print(datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'), end=" ")
    print("### ERROR ###", end=" ")
    print(*args, **kwargs)

def bisect_first_key_modified(a, x, lo=0, hi=None, reverse=False):
    if hi is None:
        hi = len(a)
    if hi == 0:
        return 0

    if reverse:
        while lo < hi:
            mid = (lo+hi)//2
            if a[mid][0] == x:
                return mid
            if a[mid][0] > x: lo = mid+1
            else: hi = mid
        return lo
    else:
        while lo < hi:
            mid = (lo+hi)//2
            if a[mid][0] == x:
                return mid
            if a[mid][0] < x: lo = mid+1
            else: hi = mid
        return lo

def getMdPushZmqSocket():
    endpoint="ipc:///home/ubuntu/tmp/.md_server_private_zmq_address"
    ctx = zmq.Context()
    socket= ctx.socket(zmq.PUSH)
    socket.connect(endpoint)
    return socket