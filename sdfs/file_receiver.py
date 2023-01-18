import socket
from typing import Tuple
# from concurrent.futures import ThreadPoolExecutor
from threading import Thread
import logging
import os
import shutil
from utility.utils import serialize

class FileReceiver:
    def __init__(self, tmp_dir: str, addr: Tuple[str, int], backlog: int=5, buff_size: int=1024, format: str='utf-8'):
        self.tmp_dir = tmp_dir
        self.addr = addr
        self.backlog = backlog
        self.buff_size = buff_size
        self.format = format

        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)

        self.connect()

    def connect(self):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.bind(self.addr)
    
    def listen(self):
        self.server_sock.listen(self.backlog)
        # executor = ThreadPoolExecutor(self.backlog)
        while True:
            conn, _ = self.server_sock.accept()
            Thread(target=self.process, args=(conn,)).start()
    
    def process(self, conn: socket.socket):
        # Receive file's meta data
        meta_chunk_size = int.from_bytes(conn.recv(4), byteorder='big')
        data = conn.recv(meta_chunk_size).decode(self.format)


        item = data.split("&")
        filename = item[0]
        file_size = int(item[1])

        logging.debug("Stream receive file name: {}".format(filename))

        # Ack
        conn.send("Filename and filesize received".encode(self.format))

        # Wrtie file in streaming based
        with open(self.tmp_dir + filename, "wb") as f:
            while True:
                chunk_size = int.from_bytes(conn.recv(4), byteorder='big')
                data = conn.recv(chunk_size)
                
                while len(data) < chunk_size:
                    to_read = chunk_size - len(data)
                    data += conn.recv(to_read)

                if not data:
                    break
                f.write(data)
                # f.write(data.decode(self.format))

        logging.debug("Stream write file complete {}".format(filename))
        conn.send(b"ACK")

        """ Closing connection. """
        conn.close()
