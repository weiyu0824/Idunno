import socket
from typing import List, Tuple
from queue import Queue
import time
from threading import Thread
import logging

from sdfs.file_system import FileSystem
from coordinator.job import Job, Task
from coordinator.job_tracker import JobTracker
from coordinator.resource_tracker import ResourceTracker
from utility.utils import serialize, deserialize
from utility.phone import Phone
from utility.config import MAIN_SCHEDULER_HOSTNAME, BACKUP_SCHEDULER_HOSTNAME, JOB_PORT, READ_BUFFER_SIZE, TASK_PORT, TASK_RESULT_PORT, CLIETN_PORT, QUERY_PORT
from group.node import Node

class QueryType:
    RATE = 1 # c1
    PROG = 2 # c2
    STATS = 3
    VM = 4

class Scheduler:

    def __init__(self, curr_node: Node, fs: FileSystem):
        # Node info
        self.curr_node = curr_node
        self.fs = fs

        # State
        self.is_started = False

        self.type = "W" # Types are W for Worker, P for primary coordinator, and B for backup coordinator

        # Tracker
        self.job_tracker = JobTracker(self.complete_job)
        self.resource_tracker = ResourceTracker()

        # Phone
        self.job_phone = Phone((curr_node.hostname, JOB_PORT), self.receive_job)
        self.query_phone = Phone((curr_node.hostname, QUERY_PORT), self.receive_query)
        self.task_result_phone = Phone((curr_node.hostname, TASK_RESULT_PORT), self.receive_task_result)

        logging.debug("Current node is {}".format(self.curr_node.hostname))
        if (self.curr_node.hostname != MAIN_SCHEDULER_HOSTNAME and self.curr_node.hostname != BACKUP_SCHEDULER_HOSTNAME):
            return
        elif(self.curr_node.hostname != MAIN_SCHEDULER_HOSTNAME):
            self.type = "P"
        elif(self.curr_node.hostname != BACKUP_SCHEDULER_HOSTNAME):
            self.type = "B"

    def start(self):
        if not self.is_started and not self.type == "W":
            Thread(target=self.job_phone.listen).start()
            Thread(target=self.task_result_phone.listen).start()
            Thread(target=self.query_phone.listen).start()
            Thread(target=self.schedule).start()
            self.is_started = True
            print("Start the Scheduler")

    def receive_job(self, conn: socket.socket, addr: Tuple[str, int]):
        logging.debug("Receive a job")
        data = conn.recv(READ_BUFFER_SIZE)
        metadata = deserialize(data)
        job = Job(self.fs, metadata, addr[0])
        
        if not job.validate():
            conn.send(serialize(False))
        else:
            self.job_tracker.sumbit_job(job)
            conn.send(serialize(True))
        conn.close()

        logging.debug(job.job_name)

    def receive_task_result(self, conn: socket.socket, addr: Tuple[str, int]):
        """
        Tell job tracker that some task is complete, and check if some job is complete
        """
        data = conn.recv(READ_BUFFER_SIZE)
        results = deserialize(data)
        conn.send(serialize(True))
        conn.close()

        work_node = results[0]        
        self.job_tracker.receive_result(work_node, results[1], results[2])
        self.resource_tracker.release_resources(work_node)
        

    def receive_query(self, conn: socket.socket, addr: Tuple[str, int]):
        data = conn.recv(READ_BUFFER_SIZE)
        query = deserialize(data)
        job_name: str = query[0]
        query_type: int = query[1]
        stats = {"result": "bad query.."}

        if self.job_tracker.contain_job(job_name):
            if query_type == QueryType.RATE:
                stats = self.job_tracker.get_rate(job_name)
            elif query_type == QueryType.PROG: 
                stats = self.job_tracker.get_progress(job_name)
            elif query_type == QueryType.STATS:
                stats = self.job_tracker.get_stats(job_name)
            elif query_type == QueryType.VM:
                stats = self.job_tracker.get_vms(job_name)
        
        conn.sendall(serialize(stats))
        conn.close()


    def assign_task(self, resource: Node, task: Task):
        logging.debug("Assign task {} to node {}".format((task.job_name, task.task_id), resource.hostname))
        logging.info("Start time of job in assign task: {}".format(task.start_time))
        task.start_time = time.time()
        self.job_tracker.allocate_resource(task, resource)
        self.resource_tracker.occupy_resources(resource)
        if self.type == "P": # only the primary node assigns tasks
            sock = socket.socket()
            try:
                sock.connect((resource.hostname, TASK_PORT))
                data = serialize(task)
                sock.sendall(data)
                # _ = deserialize(sock.recv(READ_BUFFER_SIZE))
                sock.close()
                # self.job_tracker.allocate_resource(task, resource)
                # self.resource_tracker.occupy_resources(resource)
            except ConnectionError:
                pass

    def schedule(self) -> bool:
        while True: 
            # logging.debug("[Schedule]")
            resource = self.resource_tracker.get_free_resource()
            task = self.job_tracker.get_task()
            
            # # Debug
            # if resource == None:
            #     logging.debug("No Resource")
            # else:
            #     logging.debug("Yes Resource")
            # if task == None:
            #     logging.debug("No task")
            # else:
            #     logging.debug("Yes task")

            if resource == None or task == None:
                time.sleep(1)
                continue
            
            self.assign_task(resource, task)
    
    def complete_job(self, job: Job):
        """
        Complete job and send result back to client
        """
        sock = socket.socket()
        sock.connect((job.sender, CLIETN_PORT))
        sock.send(serialize((job.job_name, job.job_result)))
        # _ = sock.recv(READ_BUFFER_SIZE)
        sock.close()

    def on_recv_membership_update(self, latest_membership_list: List[Node]):
        """
        Update our resouce tracker, and tell job tracker that some resouces in remove
        """
        if len(latest_membership_list) == self.resource_tracker.num_resources():
            return
        else:
            fail_nodes = self.resource_tracker.update_resource(latest_membership_list)
            for node in fail_nodes:
                # start_fail_time = time.time()
                self.job_tracker.remove_resource(node)
                # end_fail_time = time.time()
                if (node.hostname == MAIN_SCHEDULER_HOSTNAME and self.curr_node.hostname == BACKUP_SCHEDULER_HOSTNAME):
                    self.type = "P"
                # logging.info("Time to recover from failure: {}".format(str(end_fail_time-start_fail_time)))
