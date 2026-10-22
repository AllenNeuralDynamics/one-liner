from one_liner.server import RouterServer
from time import sleep


import logging
logging.basicConfig(level=logging.DEBUG)


server = RouterServer()
server.run()  # Start broadcast and rpc threads.
try:
    while True:
        sleep(0.1)
finally:
    server.close()
