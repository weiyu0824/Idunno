from typing import List, Set, Dict
import math
import logging
import time
import numpy as np
import json

from sdfs.file_system import FileSystem
from utility.config import DOWNLOAD_DIR
from group.node import Node

def gen_job_metadata(job_name: str, data_set: List[str], model_name: str, batch_size: int):
    return (job_name, data_set, model_name, batch_size)

class Task:
    def __init__(self, job_name: str, model_name: str, task_id: int, data_set: List[str]):
        
        self.job_name = job_name
        self.model_name = model_name
        self.task_id = task_id
        self.data_set = data_set

        # Metrics
        self.init_time = time.time()
        self.start_time = 0
        self.end_time = 0
    
    def __hash__(self):
        return hash((self.job_name, self.task_id))

    def __eq__(self, other: object) -> bool:
        return str(self) == str(other)
    
    def __str__(self):
        return self.job_name + str(self.task_id)
    
class Job:
    """
    Description: 
        This class stores information about the job, including files names, model, and batch size
    """
    def __init__(self, fs: FileSystem, metadata: tuple, sender: str):
        #
        self.fs = fs

        self.job_name: str = metadata[0]
        # self.data_set_filename = metadata[1]
        self.model_name: str = metadata[2] # model type
        self.batch_size: int = metadata[3] # batch size
        self.sender = sender
        self.data_set: List[str] = metadata[1] # list of names of items (e.g. images) to infer


        # Tasks
        self.completed_tasks: List[Task] = []
        self.processed_tasks: List[Task] = []
        self.remaining_tasks: List[Task] = []
        
        #
        self.resource_assignment: Dict[Node, Task] = {}
        self.job_result: List[List[str]] = []
        self.is_completed = False

        # stats
        self.running_time = []
        self.init_time = time.time()
        self.start_time = 0
        self.end_time = 0
        # num of completed
        # num of vms per job
        # 

        if self.validate():
            self.num_task = self.prepare()
            logging.debug("This job {} have {} tasks".format(self.job_name, self.num_task))
    
    
    def get_query_rate(self):
        currTime = time.time()
        recentTasks = []
        for completedTask in self.completed_tasks:
            if (currTime - completedTask.end_time) < 100: # last 10 seconds
                taskTime = completedTask.end_time - completedTask.start_time
                recentTasks.append(taskTime)
        numRecentTasks = 0
        for completedTask in self.completed_tasks:
            if (currTime - completedTask.end_time) < 10: # last 10 seconds
                numRecentTasks += 1
        if len(recentTasks) == 0:
            return { 
                "batch runtime" : 0,
                "num recent tasks": 0
            }
        return { 
            "batch runtime" : np.mean(recentTasks),
            "num recent tasks": numRecentTasks
        }

    def get_progress(self):
        return {
            "comleted task": len(self.completed_tasks),
            "total task": self.num_task
        }

    def get_stats(self):
        if len(self.running_time) == 0:
            return {
                "std": 0, 
                "avg": 0, 
                "percentile_90": 0, 
                "percentile_95": 0, 
                "percentile_99": 0,
                "meadian": 0
        }
        std = np.std(self.running_time)
        avg = np.mean(self.running_time)
        median = np.median(self.running_time)
        percentile_90 = np.percentile(self.running_time, 90)
        percentile_95 = np.percentile(self.running_time, 95)
        percentile_99 = np.percentile(self.running_time, 99)
        return {
            "std": std, 
            "avg": avg, 
            "percentile_90": percentile_90, 
            "percentile_95": percentile_95, 
            "percentile_99": percentile_99,
            "meadian": median
        }
    
    def get_vms(self): 
        return list(self.resource_assignment.copy().keys())


    def prepare(self):
        num_task = math.ceil(len(self.data_set) / self.batch_size)

        cursor = 0
        for task_id in range(num_task):
            data_size = min(self.batch_size, len(self.data_set) - cursor)
            task = Task(self.job_name, self.model_name, task_id, self.data_set[cursor : cursor + data_size])
            self.remaining_tasks.append(task)
            cursor += data_size
        
        return num_task
    
    def validate(self) -> bool:
        #
        # if not self.fs.download_file(self.data_set_filename):
        #     return False
        
        # time.sleep(15)
        # with open(DOWNLOAD_DIR + self.data_set_filename, 'r') as f:
        #     # data = f.readlines()[0] # ["asd", "asdas"]
        #     # self.data_set = ",".join(data).split(",")
        #     print(f.readline())
        #     self.data_set = json.load(f)

        if self.batch_size > len(self.data_set):
            return False
        else:
            return True
    
    def remove_resource(self, resource: Node):
        task = self.resource_assignment[resource]
        self.processed_tasks.remove(task)
        self.remaining_tasks.append(task)

    def get_task(self) -> Task:
        if len(self.remaining_tasks) == 0:
            # logging.debug("Threre is no task right now")
            return None
        else: 
            task = self.remaining_tasks[0]
            # logging.debug("Threre is a task {}-{}".format(task.job_name, task.task_id))
            return task
    
    def process_task(self, task: Task, resource: Node):
        logging.debug("Processs task: {}".format(str(task)))

        if len(self.remaining_tasks) == self.num_task:
            self.start_time = time.time()
            logging.info("Time to start a job: {}".format(self.start_time-self.init_time))



        task.start_time = time.time()
        self.remaining_tasks.remove(task)
        self.processed_tasks.append(task)
        self.resource_assignment[resource] = task

    def receive_result(self, work_node: Node, task: Task, result: List[str]):
        task.end_time = time.time()
        logging.info("Task time informaiton: Init={}, Start={} End={}".format(task.init_time, task.start_time, task.end_time))
        self.running_time.append((task.end_time - task.start_time))

        # for t in self.processed_tasks:
            # print("Processed task {}-{}".format(t.job_name, t.task_id))
        if task in self.processed_tasks:
            self.processed_tasks.remove(task)
            self.completed_tasks.append(task)
            # TODO: This result is unorder
            self.job_result += result
            if work_node in self.resource_assignment:
                self.resource_assignment.pop(work_node)

            if len(self.processed_tasks) == 0 \
                and len(self.remaining_tasks) == 0 \
                and len(self.completed_tasks) == self.num_task:
                self.is_completed = True
            




# ds = ["a", "b", "c", "d", "r", "e", "d", "s", "z", "q", "r"]
# model_name = "res"
# batch_size = 3
# metadata = gen_job_metadata(ds, model_name, batch_size)
# job = Job(metadata)
# for task in job.remaining_tasks:
#     print(task.task_id, task.data_set)
