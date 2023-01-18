import socket
from typing import Tuple , Callable

class Phone:
    "The class that support tcp connection"
    def __init__(self, addr: Tuple[str, int], listener: Callable[[socket.socket, Tuple[str, int]], None]):
        self.listener = listener
        self.phone_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.phone_sock.bind(addr)
        self.addr = addr

    def listen(self):
        # print("{} start listening for message".format(self.addr))
        self.phone_sock.listen()
        while True:
            conn, addr = self.phone_sock.accept()
            self.listener(conn, addr)