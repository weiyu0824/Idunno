from threading import Thread
import logging
import socket 
from typing import Tuple
import shutil, random, os
import json

from sdfs.file_system import FileSystem
from group.failure_detector import FailureDetector
from group.node import Node, NodeState
from coordinator.scheduler import Scheduler, QueryType
from coordinator.worker_mgr import WorkMgr
from coordinator.job import gen_job_metadata
from utility.phone import Phone
from utility.utils import get_hostname, get_time, serialize, deserialize, get_rand_str
from utility.config import BACKUP_SCHEDULER_HOSTNAME, MAIN_SCHEDULER_HOSTNAME, QUERY_PORT, READ_BUFFER_SIZE, JOB_PORT, CLIETN_PORT


class Core:

    # Module
    

    def __init__(self, hostname: str, timestamp: int):
        self.curr_node = Node(hostname, timestamp, NodeState.ALIVE, 0)
        self.fd = FailureDetector(hostname, timestamp)
        self.fs = FileSystem(self.curr_node)
        self.scheduler = Scheduler(self.curr_node, self.fs)
        self.worker_mgr = WorkMgr(self.curr_node, self.fs)
       
        listeners = [self.fs.on_recv_membership_update,
                    self.scheduler.on_recv_membership_update]
        self.fd.register_listener(listeners)
        self.is_started = False

        # phone
        self.client_phone = Phone((hostname, CLIETN_PORT), self.receive_result)
    
    def init(self):
        Thread(target=self.client_phone.listen).start()
    
    def receive_result(self, conn: socket.socket, addr: Tuple[str, int]):
        data = conn.recv(READ_BUFFER_SIZE)
        conn.close()
        result = deserialize(data)
        print("Result: ")
        print(result[0])
        print(result[1])
        print(len(result[1]))

    # query commands for core's state
    def list_member(self):
        for member in self.fd.membership_list:
            print(str(member))
    
    def list_self(self):
        print(str(self.curr_node))

    def join(self):
        """
        Join failure detector, Start file system, Start Scheduler
        """

        self.fd.join()
        self.fs.start()
        self.scheduler.start()
        self.worker_mgr.start()
        self.init()
        self.is_started = True

    def leave(self):
        self.fd.leave()
        self.fs.stop()

    def upload_file(self, file_path: str, sdfs_filename: str):
        return self.fs.upload_file(file_path, sdfs_filename)

    def download_file(self, sdfs_filename):
        return self.fs.download_file(sdfs_filename)
    
    def show_store(self):
        self.fs.show_store()
    
    def query(self, job_name: str, query_type: int):
        if not self.is_started:
            print("Please first join the p2p system")
            return
        sock = socket.socket()
        try:
            sock.connect((MAIN_SCHEDULER_HOSTNAME, QUERY_PORT))
            sock.send(serialize((job_name, query_type)))
            data = sock.recv(READ_BUFFER_SIZE)
            stats = deserialize(data)
        except ConnectionError:
            logging.error("Could not connect to primary coordinator")
        sock.close()

        sock = socket.socket()
        try:
            sock.connect((BACKUP_SCHEDULER_HOSTNAME, QUERY_PORT))
            sock.send(serialize((job_name, query_type)))
            data = sock.recv(READ_BUFFER_SIZE)
            stats = deserialize(data)
        except ConnectionError:
            logging.error("Could not connect to backup coordinator")
        sock.close()

        print(stats)

    def query_result(self):
        pass
    
    def inference(self, num_data: int, model_name: str, batch_size: int, data_set_folder: str=""):
        # fake_data = ["a", "b", "c", "d", "e", "f", "a", "b", "c", "d", "e", "f", 
        #     "b", "c", "d", "e", "f", "a", "b", "c", "d", "e", "f", "b", "c", "d", 
        #     "e", "f", "a", "b", "c", "d", "e", "f", "b", "c", "d", "e", "f", "a", "b", "c", "d", "e", "f"]

        dir_path = 'data/imgnet_data'

        filenames = random.sample(os.listdir(dir_path), num_data)
        for filename in filenames:
            file_path = os.path.join(dir_path, filename)
            # self.fs.upload_file(file_path, filename)

        # store data set to sdfs
        rand_job_name = get_rand_str()
        # data_set_filename = f'{rand_job_name}_dataset.json'
        # with open(data_set_filename, 'w') as f:
        #     json.dump(filenames, f)
        # self.upload_file(data_set_filename, data_set_filename)

        # send job meta data to the scheduler     
        job_param = gen_job_metadata(rand_job_name, filenames, model_name, batch_size)

        sock = socket.socket()
        try:
            sock.connect((MAIN_SCHEDULER_HOSTNAME, JOB_PORT))
            sock.send(serialize(job_param))
            ack = deserialize(sock.recv(READ_BUFFER_SIZE))
        except ConnectionError:
            logging.error("Could not connect to primary coordinator")
        sock.close()

        sock = socket.socket()
        try:
            sock.connect((BACKUP_SCHEDULER_HOSTNAME, JOB_PORT))
            sock.send(serialize(job_param))
            ack = deserialize(sock.recv(READ_BUFFER_SIZE))
        except ConnectionError:
            logging.error("Could not connect to backup coordinator")
        sock.close()

        if ack == False:
            print("This job is not validate!")
        else:
            print("Your job name: {}".format(rand_job_name))

def command_line(core: Core):
    while True:
        args = input("Type a command: ").split(' ')
        cmd = args[0]
        num_args = len(args)

        if cmd == "help":
            print("""\

Usage: 
    list_mem                                Display membership list
    list_self                               Display meta data of current node
    join                                    Join the p2p system
    leave                                   Leave the p2p system
    put [file path] [sdfs file name]        Upload a local file to the sdfs
    get [sdfs file name]                    Download a file from sdfs
    q_result [job name]                     Query the result of a job
    q_rate [job name]                       Query the avg query rate of a job (10 s)
    q_prog [job name]                       Query the progress of a job
    q_stats [job name]                      Query the stas of a job
    q_vm [job name]                         Query vm that is assigned to a job now
    infer [model name] [data set folder]    Inference a model
            """) 
        elif cmd == "list_mem":
            core.list_member()
        elif cmd == "list_self":
            core.list_self()
        elif cmd == "join":
            core.join()
        elif cmd == "leave":
            core.leave()
        elif cmd == "put" and num_args == 3:
            core.upload_file(args[1], args[2])
        elif cmd == "get" and num_args ==  2:
            core.download_file(args[1])
        elif cmd == "store":
            core.show_store()
        elif cmd == "q_result" and num_args ==  2:
            pass
        elif cmd == "q_rate" and num_args ==  2:
            core.query(args[1], QueryType.RATE)
        elif cmd == "q_prog" and num_args ==  2:
            core.query(args[1], QueryType.PROG)
        elif cmd == "q_stats" and num_args ==  2:
             core.query(args[1], QueryType.STATS)
        elif cmd == "q_vm" and num_args ==  2:
            core.query(args[1], QueryType.VM)
        elif cmd == "infer" and num_args == 4:
            core.inference(num_data = int(args[1]), model_name = args[2], batch_size = int(args[3]))
        else:
            print("This command is not valid")

if __name__ =="__main__":
    if os.path.exists('core.log'):
        os.remove('core.log')
    logging.basicConfig(filename='core.log', level=logging.DEBUG)
    logging.debug('[Core] Start Logging ...')

    hostname = get_hostname()
    timestamp = get_time()
    print(hostname)
    print(timestamp)

    idduno_core = Core(hostname, timestamp)

    Thread(target=command_line, args=(idduno_core,)).start()
