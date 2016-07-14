import socket
import json

def run_worker(handle_call, hostname="", port=48484):
    with socket.socket() as s:
        s.connect((hostname, port))
        sfile = s.makefile("rw")
        while True:
            call = json.loads(sfile.readline())
            result = handle_call(call)
            sfile.write(json.dumps(result) + "\n")
            sfile.flush()

