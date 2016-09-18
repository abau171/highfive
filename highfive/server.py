import asyncio
import json


class RemoteWorker:

    def __init__(self, reader, writer):

        self._reader = reader
        self._writer = writer

    async def make_call(self, call):

        call_json = json.dumps(call) + "\n"
        call_encoded = call_json.encode("utf-8")
        self._writer.write(call_encoded)

        response_encoded = await self._reader.readuntil(b"\n")
        response_json = response_encoded.decode("utf-8")
        response = json.loads(response_json)

        return response

    def close(self):

        self._writer.close()


class Server:

    def __init__(self, host, port, manager, *, loop):

        self._host = host
        self._port = port
        self._manager = manager
        self._loop = loop

        self._server = None

    def _accept(self, reader, writer):

        worker = RemoteWorker(reader, writer)
        self._manager.add_worker(worker)

    async def start(self):

        self._server = await asyncio.start_server(
            self._accept, host=self._host, port=self._port, loop=self._loop)

    def close(self):

        self._server.close()

    async def wait_closed(self):

        await self._server.wait_closed()


async def start_server(host, port, manager, *, loop):

    s = Server(host, port, manager, loop=loop)
    await s.start()
    return s

