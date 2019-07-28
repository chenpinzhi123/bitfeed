sudo service mongod restart
nohup python -u bitmex_md.py -s XBTUSD,XBTM19,XBT7D_U105,XBT7D_D95,ETHUSD &
nohup python -u md_server.py &
nohup python -u md_client_mongo.py &
