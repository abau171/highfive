import socket
import time

import highfive


with highfive.Master("", 48484) as m:
    s = socket.socket()
    s.connect(("", 48484))
    time.sleep(2)
    s.close()
    print("closing...")
print("closed")

