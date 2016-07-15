import multiprocessing
import socket
import json

def run_worker(handle_call, hostname="", port=48484):
    with socket.socket() as s:
        s.connect((hostname, port))
        sfile = s.makefile("rw")
        try:
            while True:
                call = json.loads(sfile.readline())
                result = handle_call(call)
                sfile.write(json.dumps(result) + "\n")
                sfile.flush()
        except json.JSONDecodeError:
            pass
        finally:
            sfile.close()

def run_workers(handle_call, hostname="", port=48484, count=0):
    if count == 0:
        count = multiprocessing.cpu_count()
    procs = []
    for _ in range(count):
        proc = multiprocessing.Process(
            target=run_worker, args=(handle_call, hostname, port))
        procs.append(proc)
        proc.start()
    for proc in procs:
        proc.join()

