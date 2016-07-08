"""Socket wrapper which allows for string message-passing."""

import json


class Closed(Exception):
    """Exception raised when the connection is closed.

    This occurs when an error is raised during sending or receiving a
    message.

    """

    pass


class MessageConnection:
    """Wraps sockets to use string message-passing for connections."""

    def __init__(self, client_socket):
        self._client_socket = client_socket
        self._socket_file = self._client_socket.makefile("rw")
        self._closed = False

    def is_closed(self):
        """Determines whether the connection is closed."""
        return self._closed

    def send(self, message):
        """Attempts to send a string message.

        Raises a Closed exception if an error which can close the connection
        occurs.

        """

        try:
            self._socket_file.write(json.dumps(message) + "\n")
            self._socket_file.flush()
        except (ConnectionError, TimeoutError, TypeError) as e:
            self._closed = True
            raise Closed from e

    def recv(self):
        """Attempts to receive a string message from the remote connection.

        Raises a Closed exception if an error which can close the connection
        occurs.

        """

        try:
            encoded = self._socket_file.readline()
            message = json.loads(encoded)
            return message
        except (ConnectionError, TimeoutError, json.JSONDecodeError) as e:
            self._closed = True
            raise Closed from e

