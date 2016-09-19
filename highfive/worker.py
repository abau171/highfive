import asyncio
import multiprocessing
import json
import logging


logger = logging.getLogger(__name__)


async def handle_jobs(job_handler, host, port, *, loop):

    try:

        try:
            reader, writer = await asyncio.open_connection(host, port, loop=loop)
        except OSError:
            logging.error("worker could not connect to server")
            return

        while True:

            try:
                call_encoded = await reader.readuntil(b"\n")
            except asyncio.IncompleteReadError:
                break
            logging.debug("worker got call")
            call_json = call_encoded.decode("utf-8")
            call = json.loads(call_json)

            response = job_handler(call)

            response_json = json.dumps(response) + "\n"
            response_encoded = response_json.encode("utf-8")
            writer.write(response_encoded)
            logging.debug("worker returned response")

    except KeyboardInterrupt:

        pass


def worker_main(job_handler, host, port):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)
    loop.run_until_complete(handle_jobs(job_handler, host, port, loop=loop))
    loop.close()


def run_worker_pool(job_handler, host="localhost", port=48484,
                      *, max_workers=None):

    if max_workers is None:
        max_workers = multiprocessing.cpu_count()

    processes = []
    for _ in range(max_workers):
        p = multiprocessing.Process(target=worker_main,
                args=(job_handler, host, port))
        p.start()
        processes.append(p)

    logger.debug("workers started")

    for p in processes:
        p.join()

    logger.debug("all workers completed")

