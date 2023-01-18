import socket
from typing import Tuple, List
import logging
# from concurrent.futures import ThreadPoolExecutor
from threading import Thread

from utility.utils import serialize, deserialize
from utility.phone import Phone
from coordinator.job import Task
from coordinator.worker import Worker
from group.node import Node
from utility.config import TASK_PORT, READ_BUFFER_SIZE, TASK_RESULT_PORT, BACKUP_SCHEDULER_HOSTNAME, MAIN_SCHEDULER_HOSTNAME
from sdfs.file_system import FileSystem


class WorkMgr:
    def __init__(self, curr_node: Node, fs: FileSystem, num_worker: int=1):
        # TODO: multiple worker
        self.curr_node = curr_node
        self.workers = [Worker(fs, self.finish_task) for _ in range(num_worker)]
        self.free_worker = num_worker
        self.results = []
        
        self.task_phone = Phone((curr_node.hostname, TASK_PORT), self.receive_task)

        self.is_started = False
    
    def start(self):
        if not self.is_started:
            Thread(target=self.task_phone.listen).start()
            self.is_started = True
            print("Start the Worker Manager")


    def receive_task(self, conn: socket.socket, addr: Tuple[str, int]):
        data = conn.recv(READ_BUFFER_SIZE)
        task: Task = deserialize(data)

        logging.debug("Receive a task (job name={}, task_id={})".format(task.job_name, task.task_id))

        if self.free_worker >= 0:
            logging.debug("There is a free worker")
            Thread(self.workers[0].work(task)).start()
            self.free_worker -= 1
            # conn.send(serialize(True))
        # else:
            # conn.send(serialize(False))
        conn.close()

    def finish_task(self, task: Task, result: List[str]):
        logging.debug("Finish Task")
        sock = socket.socket()
        try:
            sock.connect((MAIN_SCHEDULER_HOSTNAME, TASK_RESULT_PORT))
            sock.send(serialize((self.curr_node, task, result)))
            _ = sock.recv(READ_BUFFER_SIZE)
            sock.close()
        except ConnectionError:
            logging.error("Could not connect to primary coordinator")
        sock.close()
        
        sock = socket.socket()
        try:
            sock.connect((BACKUP_SCHEDULER_HOSTNAME, TASK_RESULT_PORT))
            sock.send(serialize((self.curr_node, task, result)))
            _ = sock.recv(READ_BUFFER_SIZE)
        except ConnectionError:
            logging.error("Could not connect to backup coordinator")
        sock.close()

        self.free_worker += 1
                    
