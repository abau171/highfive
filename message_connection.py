import json


class Closed(Exception):
    pass


class MessageConnection:

    def __init__(self, client_socket):
        self._client_socket = client_socket
        self._socket_file = self._client_socket.makefile("rw")
        self._closed = False

    def is_closed(self):
        return self._closed

    def send(self, message):
        try:
            self._socket_file.write(json.dumps(message) + "\n")
            self._socket_file.flush()
        except (ConnectionError, TimeoutError, TypeError) as e:
            self._closed = True
            raise Closed from e

    def recv(self):
        try:
            encoded = self._socket_file.readline()
            message = json.loads(encoded)
            return message
        except (ConnectionError, TimeoutError, json.JSONDecodeError) as e:
            self._closed = True
            raise Closed from e

