from collections import defaultdict
from threading import Lock, Thread
# from concurrent.futures import ThreadPoolExecutor
import socket
import logging
import shutil
from typing import List, Tuple
import os
import time
import copy

from sdfs.file_receiver import FileReceiver
from sdfs.file_sender import FileSender
from sdfs.file_message import FileMessage, FileMessageType
from sdfs.transaction import GetTransaction, PutTransaction, DeleteTransaction, ListTransaction, VersionTransaction
from sdfs.transaction import TransactionParam, TrasanctionType
from group.node import Node
from utility.utils import serialize, deserialize
from utility.phone import Phone
from utility.config import READ_BUFFER_SIZE, HASH_M, NUM_REPLICA
from utility.config import DOWNLOAD_DIR, CACHE_DIR, SDFS_DIR
from utility.config import SERVICE_PORT, FILE_PORT, STREAM_PORT

def get_filename(filename: str, version: int):
    return filename + "-" + str(version)

class FileSystem:
    def __init__(self, curr_node: Node):

        # Node info
        self.curr_node = curr_node

        # Sdfs State
        self.is_started = False

        # Membership List
        # TODO: use Read Write Lock
        self.membership_lock = Lock()
        self.membership_list: List[Node] = []
        self.successors: List[Node] = []
        self.predecessor: Node = None

        # File system meta data
        self.main_files = defaultdict(lambda:  0) # filename, version
        self.replicated_files = defaultdict(lambda:  0) # filename, version
        self.main_files_lock = Lock()
        self.replicated_files_lock = Lock()

        # Phone
        self.txn_phone = Phone((curr_node.hostname, SERVICE_PORT), self.receive_txn)
        self.file_msg_phone = Phone((curr_node.hostname, FILE_PORT), self.receive_file_msg)

        # Stream file transfer
        self.file_sender = FileSender()
        self.file_receiver = FileReceiver(CACHE_DIR, (curr_node.hostname, STREAM_PORT))

        # Thread Pool
        # self.executor = ThreadPoolExecutor(5)

        self.clear_dir()

    def clear_dir(self):
        # Remove original directory and create a new one
        if os.path.exists(SDFS_DIR):
            shutil.rmtree(SDFS_DIR)
        if os.path.exists(DOWNLOAD_DIR):
            shutil.rmtree(DOWNLOAD_DIR)
        os.makedirs(SDFS_DIR)
        os.makedirs(DOWNLOAD_DIR)

    def start(self):
        if not self.is_started:
            Thread(target=self.file_receiver.listen).start()
            Thread(target=self.txn_phone.listen).start()
            Thread(target=self.file_msg_phone.listen).start()
            self.is_started = True
            print("Start the File System")

    def stop(self):
        ## TODO: make sure to close threads and socket
        os._exit(1)

    ###############
    # Client-part #
    ###############
    # Methods that can operate this file system
    # TODO: Send txn params need a addr
    def upload_file(self, file_path: str, sdfs_filename: str, attempts: int=3) -> None:
        if not self.is_started:
            print("Please join first, SDFS has not start yet!")
            return False

        if not os.path.exists(file_path):
            print("{} does not exist".format(file_path))
            return False
        logging.debug("Already join and file exist!!!!")
        success = False
        for _ in range(attempts):
            # send file to the main_node
            main_node = self.get_main_node(sdfs_filename)
            self.file_sender.send_file(file_path, sdfs_filename, (main_node.hostname, STREAM_PORT))
            txn_params = TransactionParam.genPutTxnParams(sdfs_filename)
            success = self.send_txn_params(main_node, txn_params)

            if success: break
            else: time.sleep(0.5)
        
        if not success:
            print("Failed!")
            return False
        return True

    def download_file(self, sdfs_filename: str, attempts: int=3):
        if not self.is_started:
            print("Please join first, SDFS has not start yet!")
            return False

        success = False
        for _ in range(attempts):
            # request file from server
            main_node = self.get_main_node(sdfs_filename)
            txn_params = TransactionParam.genGetTxnParams(sdfs_filename)
            success = self.send_txn_params(main_node, txn_params)
            if success: break
            else: time.sleep(1)

        if success:
            try:
                shutil.move(CACHE_DIR + sdfs_filename, DOWNLOAD_DIR)
                return True
            except shutil.Error as err:
                logging.error(err)            
            # print("Succesfully downloaded file '{}' from SDFS".format(sdfs_filename))
        else:
            print("Fail!") 
        return False

    def delete_file(self, sdfs_filename: str, attempts: int=3):
        pass
        # if not self.is_started:
        #     print("Please join first, SDFS has not start yet!")
        #     return

        # success = False
        # for _ in range(attempts):
        #     main_node = self.get_main_node(sdfs_filename)
        #     txn_params = TransactionParam.genDeleteTxnParams(sdfs_filename)
        #     success = self.send_tx_params(txn_params)
        #     if success: break
        #     else: time.sleep(0.5)
        
        # if success:
        #     print("Succesfully deleted {} from SDFS".format(sdfs_filename))
        # else:
        #     print("Fail!") 

    def list_addr(self):
        pass

    def show_store(self):
        if not self.is_started:
            print("Please join first, SDFS has not start yet!")
            return
        print("Main:")
        for filename in self.main_files.keys():
            print("  {}".format(filename))
        print("Replicated:")
        for filename in self.replicated_files.keys():
            print("  {}".format(filename))
        return

    def donwload_vfile(self, attempts: int=3):
        if not self.is_started:
            print("Please join first, SDFS has not start yet!")
            return
    
    ###############
    # Handler
    ###############
    # Methods that handle sdfs-related transactions

    def handle_upload(self, put_txn: PutTransaction):
        """
        Put file to all replica, be sure every replica have file before commit
        """
        
        #TODO: do we need locking mechnism
        filename = put_txn.sdfs_filename
        num_replica = min(len(self.membership_list) - 1, NUM_REPLICA)
        logging.info("There should have {} replica of the file".format(num_replica))
        num_success = 0
        success_nodes: List[Node] = []

        with self.membership_lock:
            logging.debug("Listing all the successors")
            for node in self.successors:
                logging.debug(node.hostname)

            for node in self.successors: #TODO: use seperate thread to send file
                logging.debug("Sending replica to {}".format(node.hostname))
                file_path = CACHE_DIR + filename
                self.file_sender.send_file(file_path, filename, (node.hostname, STREAM_PORT))
                file_msg = FileMessage(FileMessageType.PUT, filename, self.main_files[filename] + 1)
                logging.debug("Sending file message to {}".format(node.hostname))
                success = self.send_file_message(node, file_msg)
                if success: 
                    num_success += 1
                    success_nodes.append(node)
                else:
                    logging.error("{} do not successfully get the file".format(node.hostname))

        logging.debug("There are {} sucess acks and should have {} replica".format(num_success, num_replica))

        # TODO: why >= instead of == 
        if num_success >= num_replica:
            self.store_file(filename, self.main_files[filename] + 1)
            return True
        else:
            for node in success_nodes:
                logging.info("Ask {} to delete laster version of file: {}".format(node.hostname, filename))
                file_msg = FileMessage(FileMessageType.DeleteOne, filename)
                self.send_file_message(node, file_msg)
            
            return False


    def handle_download(self, get_txn: GetTransaction, sender_addr: Tuple[str, int]):
        """
        Return:
            A boolean indicated if file exist on the machine, and a file itself
        """
        # TODO: Error handling at sending file
        # TODO: what is the correct remote filename in ml system?
        filename = get_txn.sdfs_filename
        addr = (sender_addr[0], STREAM_PORT)
        if filename in self.main_files:
            version = self.main_files[filename]
            file_path = SDFS_DIR + get_filename(filename, version)
            self.file_sender.send_file(file_path, filename, addr)
            return True
        else:
            return False


    def handle_delete():
        pass

    def handle_list_addr():
        pass

    def handle_vdownload():
        pass


    ###############
    # Listener
    # #############
    # Methods that listen for messages

    def receive_txn(self, conn: socket.socket, addr: Tuple[str, int]):
        data = conn.recv(READ_BUFFER_SIZE)
        txn_param = deserialize(data)

        # Log when receiving a transaction
        txn_type = txn_param[0]
        logging.info("Receive a {} Txn from {}".format(txn_type, addr))

        # Right now, we have already received the attachment
        if txn_type == TrasanctionType.Put:
            ack = self.handle_upload(PutTransaction(txn_param)) 
        elif txn_type == TrasanctionType.Get:
            ack = self.handle_download(GetTransaction(txn_param), addr)
        else:
            logging.debug("Unsupport transaction")
            ack = False
        
        logging.debug("Complete a {} Txn".format(txn_type))

        conn.sendall(serialize(ack))
        conn.close()

    def receive_file_msg(self, conn: socket.socket, addr: Tuple[str, int]):
        data = conn.recv(READ_BUFFER_SIZE)
        file_msg: FileMessage = deserialize(data)
        logging.info("Receive a {} type file message".format(file_msg.msg_type))

        success = False
        if file_msg.msg_type == FileMessageType.DeleteOne:
            success = self.delete_file(file_msg.filename)
        elif file_msg.msg_type == FileMessageType.PUT:
            success = self.store_file(file_msg.filename, file_msg.version)
        
        logging.debug("Replica sending acknowledgement")
        conn.sendall(serialize(success))
        conn.close()

    
    ##############
    # Sender
    ##############
    # Methods that send messages
    # TODO: use phone to connect and send
    def send_txn_params(self, node: Node, txn_params: tuple) -> bool:
        addr = (node.hostname, SERVICE_PORT)
        logging.info("Client send Tx params to {}".format(addr))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(addr)
        except ConnectionError:
            logging.error("Unable to connect to {}".format(addr))
            return False

        sock.sendall(serialize(txn_params))
        data = sock.recv(READ_BUFFER_SIZE)
        # sendall(sock, serialize(txn_params))
        # data = recvall(sock)
        success = deserialize(data)
        return success
    
    def send_file_message(self, node: Node, file_msg: FileMessage) -> bool:
        addr = (node.hostname, FILE_PORT)
        logging.info("Sending {} type file message to {}".format(file_msg.msg_type, addr))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(addr)
        except ConnectionError:
            logging.error("Unable to connect to {}".format(addr))
            return False

        sock.sendall(serialize(file_msg)) # TODO: timeout method
        # sendall(sock, serialize(file_msg))

        logging.info("Sending {} type file message to {} SENDING COMPLETED".format(file_msg.msg_type, addr))

        data = sock.recv(READ_BUFFER_SIZE)
        # data = recvall(sock)
        success = deserialize(data)
        logging.info("Sending {} type file message to {} COMPLETED".format(file_msg.msg_type, addr))
        return success
    

    ##############
    # Utility
    ##############
    def get_main_node(self, filename: str) -> Node:
        file_hash_code = hash(filename) % HASH_M
        with self.membership_lock:
            # logging.debug("{} has hashcode={}".format(filename, hashCode))
            main_node = self.membership_list[0]
            for member in self.membership_list:
                if file_hash_code <= member.hash_code:
                    main_node = member
                    break
        
        return main_node

    def move_file(self, filename: str, version: int):
        """Move file from cahce to sdfs"""
        file_path = CACHE_DIR + filename
        v_file_path = CACHE_DIR + get_filename(filename, version)
        os.rename(file_path, v_file_path)
        shutil.move(v_file_path, SDFS_DIR) 

    def store_file(self, filename: str, version:int) -> bool:
        """
        Store the file locally
        """
        logging.info("About to store {} locally".format(filename))

        main_node = self.get_main_node(filename)
        if main_node.hostname == self.curr_node.hostname:
            if self.main_files[filename] + 1 == version:
                self.move_file(filename, version)               
                self.main_files[filename] = version
            elif self.main_files[filename] >= version:
                logging.info("File {}, {}-th version already stored on the machine".format(filename, version))
                return True
            else:
                logging.error("You got the wrong version, we assume that we got the version 1 by 1")
                return False
        else:
            if self.replicated_files[filename] + 1 == version:
                self.move_file(filename, version)
                self.replicated_files[filename] = version
            elif self.replicated_files[filename] >= version:
                logging.info("File {}, {}-th version already stored on the machine".format(filename, version))
                return True
            else:
                logging.error("You got the wrong version, we assume that we got the version 1 by 1")
                return False

        logging.debug("Sucessfully store {} locally".format(filename))
        return True

    def delete_file(self, filename):
        """
        Delete specifically version of the file, right now just delete the lastest version
        """
        logging.info("Delete latest versions of file {}".format(filename))

        if filename in self.main_files:
            version = self.main_files[filename]
        elif filename in self.replicated_files:
            version = self.replicated_files[filename]
        else:
            print("File {} does not exist on the machine".format(filename))
            return False

        filePath = SDFS_DIR + get_filename(filename, version)
        # print(filePath)
        if os.path.isfile(filePath):
            os.remove(filePath)
        else:
            logging.error("Something went wrong, file data structure is not align with the real folder")
            return False

        if version == 1:
            if filename in self.main_files:
                self.main_files.pop(filename)
            elif filename in self.replicated_files:
                self.replicated_files.pop(filename)
        return True




    ##############
    # Rereplicate
    ##############
    def leave_adjust(self):
        """
        After adjust the membership list, we need to relocate our file.
        Some replicated files may becomes main file, and some other files would become main files or replicated files
        """
        logging.debug("Calling leave adjust")
        start = time.time()
        self.stable = False
        for filename, v in self.replicated_files.copy().items():
            if self.get_main_node(filename).hostname == self.curr_node.hostname:
                self.main_files[filename] = v
                self.replicated_files.pop(filename)

        for filename, num_version in self.main_files.items():
            self.send_files(filename, num_version, self.successors)
        end = time.time()
        logging.info("Leave Adjust Time = {}".format(end-start))
    
    def join_adjust(self):
        """
        After adjust the membership list when a new node join, we need relocate our files. 
        Whenever there is a join, some main files should become replicated files, and some files in replicated files should be removed
        """
        logging.debug("Calling join adjust")
        with self.main_files_lock:

            for filename, num_version in self.main_files.items():
                self.send_files(filename, num_version, self.successors)

            for filename, num_version in self.main_files.copy().items():
                if self.get_main_node(filename).hostname != self.curr_node.hostname:
                    # send file to predecessor
                    self.send_files(filename, num_version, [self.predecessor])
                    self.replicated_files[filename] += 1
                    logging.debug("join adjust 2. The new main node is {}".format(self.get_main_node(filename)))
                    self.main_files.pop(filename)
                # TODO: remove some of replicated files
                # TODO: 
            
    def send_files(self, filename: str, num_version: int, dests: List[Node]):
        
        for v in range(1, num_version + 1):
            logging.debug("The current successors are {}".format(self.successors))
            for node in dests:
                file_path = SDFS_DIR + get_filename(filename, v)
                dest_addr = (node.hostname, STREAM_PORT)
                self.file_sender.send_file(file_path, filename, dest_addr)
                logging.debug("Sending file to {} when re-replicating".format(dest_addr))
                file_msg = FileMessage(FileMessageType.PUT, filename, version=v)
                self.send_file_message(node, file_msg)

    def on_recv_membership_update(self, latest_membership_list: List[Node]):
        leave = False
        with self.membership_lock:
            if len(self.membership_list) > len(latest_membership_list):
                leave = True

            self.membership_list = copy.deepcopy(latest_membership_list)
            cur_idx = 0
            for idx, member in enumerate(self.membership_list):
                if member == self.curr_node:
                    cur_idx = idx
                    break
            
            num_member = len(self.membership_list)
            num_replica = min(num_member - 1, NUM_REPLICA)
            self.successors = []
            for i in range(1, num_replica + 1):
                pos = (cur_idx + i) % num_member
                self.successors.append(self.membership_list[pos])
            if num_member > 1:
                pos = (cur_idx - 1 % num_member)
                self.predecessor = self.membership_list[pos]
            else:
                self.predecessor = None
        if leave:
            self.leave_adjust()
        else:
            self.join_adjust()