import websocket
from threading import Thread
import threading
import json

import time
import datetime

import argparse

import sys
import os
bd = os.path.dirname(os.path.abspath(sys.argv[0]))
for p in [bd+"/../util",]:
    sys.path.append(p)
import util

class BITMEX_ws():
    host="wss://www.bitmex.com/realtime"

    def __init__(self, product_lists, pushSocket=None):
        self.pushSocket = pushSocket
        self.product_lists = product_lists

        self.books = {}
        self._raw_books = {}
        self._books_lock_dict = {}
        for symbol in self.product_lists:
            self._raw_books[symbol] = {'asks':{},'bids':{}}
            self.books[symbol] = {'asks':[],'bids':[]}
            self._books_lock_dict[symbol] = threading.Lock()

    def connect(self):
        self.ws = websocket.WebSocketApp(self.host,
                                 on_message=self.on_message,
                                 on_error=self.on_error,
                                 on_close=self.on_close,
                                 on_open=self.on_open) 

        self.thread = Thread(target=self.ws.run_forever,
                             kwargs={"ping_interval":5})
        self.thread.setDaemon(True)
        self.thread.start()
    
    def close(self):
        self.ws.close()
    
    def on_message(self, msg):
        msg = json.loads(msg)
        md = msg.get("data")
        action = msg.get("action")

        if action in ('partial','insert'):
            exchSym = md[0]['symbol']
            self._books_lock_dict[exchSym].acquire()
            for record in md:
                price,amount = record['price'],record['size']
                if record['side'] == 'Buy':
                    self._raw_books[exchSym]['bids'][record['id']] = price
                    array = self.books[exchSym]['bids']
                    i = util.bisect_first_key_modified(array, price, reverse=True)
                elif record['side'] == 'Sell':
                    self._raw_books[exchSym]['asks'][record['id']] = price
                    array = self.books[exchSym]['asks']
                    i = util.bisect_first_key_modified(array, price, reverse=False)
                array.insert(i, [price,abs(amount)])

            self.push_ipc(self.books[exchSym], exchSym)
            self._books_lock_dict[exchSym].release()

        elif action == 'update':
            exchSym = md[0]['symbol']
            self._books_lock_dict[exchSym].acquire()
            for record in md:
                if record['side'] == 'Buy':
                    price,amount = self._raw_books[exchSym]['bids'][record['id']],record['size']
                    array = self.books[exchSym]['bids']
                    i = util.bisect_first_key_modified(array, price, reverse=True)
                elif record['side'] == 'Sell':
                    price,amount = self._raw_books[exchSym]['asks'][record['id']],record['size']
                    array = self.books[exchSym]['asks']
                    i = util.bisect_first_key_modified(array, price, reverse=False)

                if array[i][0] != price:
                    util.log_error("UPDATE ERROR")
                    self._books_lock_dict[exchSym].release()
                    return

                array[i][1] = amount

            self.push_ipc(self.books[exchSym], exchSym)
            self._books_lock_dict[exchSym].release()

        elif action == 'delete':
            exchSym = md[0]['symbol']
            self._books_lock_dict[exchSym].acquire()
            for record in md:
                if record['side'] == 'Buy':
                    price = self._raw_books[exchSym]['bids'][record['id']]
                    del self._raw_books[exchSym]['bids'][record['id']]
                    array = self.books[exchSym]['bids']
                    i = util.bisect_first_key_modified(array, price, reverse=True)
                elif record['side'] == 'Sell':
                    price = self._raw_books[exchSym]['asks'][record['id']]
                    del self._raw_books[exchSym]['asks'][record['id']]
                    array = self.books[exchSym]['asks']
                    i = util.bisect_first_key_modified(array, price, reverse=False)

                if array[i][0] != price:
                    util.log_error("DELETE ERROR")
                    self._books_lock_dict[exchSym].release()
                    return

                if len(array)>0 and isinstance(array[i],list):
                    del array[i]
                else:
                    util.log_error("DELETE ERROR")
                    self._books_lock_dict[exchSym].release()
                    return

            self.push_ipc(self.books[exchSym], exchSym)
            self._books_lock_dict[exchSym].release()
        
        else:
            # action is None or none of the above. Ignore such input msg.
            return

    def push_ipc(self, bids_and_asks, exchSym):
        msg = {"exchange":"BITMEX",
               "exchSym":exchSym,
               "timestamp":time.time(),
               "bids":bids_and_asks["bids"],
               "asks":bids_and_asks["asks"]
               }
        if args.print:
            util.log_info(msg)
        else:
            self.pushSocket.send_string(json.dumps(msg))        

    def on_error(self, error):
        util.log_error(error)
    
    def on_close(self):
        util.log_info("### closed ###")

    def on_open(self):
        util.log_info("### opened ###")
        sub = {"op": "subscribe", "args": []}
        for symbol in self.product_lists:
            sub["args"].append("orderBookL2_25:%s"%symbol)
        self.ws.send(json.dumps(sub))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--subscribe", default="XBTUSD",
                        help="data to subscribe, e.g. XBTUSD,ETHUSD ; default: XBTUSD")
    parser.add_argument("--print", action="store_true", default=False,
                        help="print the logs")
    args = parser.parse_args()
    if args.subscribe:
        product_lists = args.subscribe.split(',')
    else:
        product_lists = ["XBTUSD"]

    pushSocket = util.getMdPushZmqSocket()

    bitmex=BITMEX_ws(product_lists=product_lists,
                     pushSocket=pushSocket)
    bitmex.connect()

    while True:
        try:
            time.sleep(1000)
        except (KeyboardInterrupt,SystemExit):
            print("KeyboardInterrupt")
            bitmex.close()
            break
        except Exception as e:
            print("Exception: ", e)
            bitmex.close()
            bitmex.connect()
            time.sleep(1)
