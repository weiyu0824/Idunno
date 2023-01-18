import socket
from typing import Tuple
import logging
import os

class FileSender:
    "This class is responsible for sending file"
    def __init__(self, buff_size: int=1024, format: str="utf-8"):
        self.buff_size = buff_size
        self.format = format

    def send_file(self, file_path: str, remote_filename: str, dest_addr: Tuple[str, int]):
        """
        Use streaming-based method to send a file
        """
        # TODO: handle socket connection break
        logging.debug("Start stream send file:{} to {}".format(file_path, dest_addr))

        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect(dest_addr)

        """ Sending the filename and filesize to the server. """
        file_size = os.path.getsize(file_path)
        encoded_meta_data = f"{remote_filename}&{file_size}".encode()
        meta_chunk_size = len(encoded_meta_data)
        client_sock.send(meta_chunk_size.to_bytes(4, byteorder='big'))
        client_sock.send(encoded_meta_data)


        logging.debug("Stream send data")
        # TODO: Why I cannot listen here
        # msg = client_sock.recv(self.buff_size).decode(self.format)
        # logging.debug(f"SERVER: {msg}")

        with open(file_path, "rb") as f:

            logging.debug("start reading file") 
            while True:
                data = f.read(self.buff_size)

                if not data:
                    break

                # encoded_data = data.encode(self.format)
                # chunk_size = len(encoded_data)
                chunk_size = len(data)
                # send chunk size
                client_sock.send(chunk_size.to_bytes(4, byteorder='big'))
                # send data
                client_sock.sendall(data)

        client_sock.recv(self.buff_size)

        logging.debug("Successfully send file:{} to {}".format(file_path, dest_addr))

        """ Closing the connection """
        client_sock.close()