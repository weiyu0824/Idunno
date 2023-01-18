import socket
from typing import Callable, Any
import logging

DROP_RATE = 0.3

class Communication:
    def __init__(self, host:str, port:int, listener:Callable[[bytes, Any], None]):
        """
        A communication module, using UDP protocal
        
        """
        # UDP
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, port))
        self.listener = listener

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(1024)
            self.listener(data, addr)

    # aquire lock before calling this function
    def send(self, data: bytes, addr: tuple):
        self.sock.sendto(data, addr)