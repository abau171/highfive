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


async def start_server(host, port, manager, *, loop):

    def accept(reader, writer):
        worker = RemoteWorker(reader, writer)
        manager.add_worker(worker)

    s = await asyncio.start_server(accept, host=host, port=port, loop=loop)
    return s

