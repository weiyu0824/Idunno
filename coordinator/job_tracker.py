from typing import Dict, List, Callable
import logging
import time

from sdfs.file_system import FileSystem
from group.node import Node
from coordinator.job import Job, Task

# hardcode avg run time
avg_runtime: Dict[str, int] = {
    "AlexNet": 2,
    "ResNet": 1
}

class JobTracker:
    def __init__(self, complete_listener: Callable[[Job], None]):
        self.jobs: Dict[str, Job] = {}
        self.resource_dists: Dict[Node, str] = {}
        self.resource_nums: Dict[str, int] = {}

        # listener for job completion
        self.complete_listener = complete_listener
    
    def cal_resource_possesion(self, job_name: str) -> float:
        """
        Formula = # resource / (batch_size * avg_runtime)
        """
        job = self.jobs[job_name]
        num_resource = self.resource_nums[job_name]
        return num_resource / (job.batch_size * avg_runtime[job.model_name])

    def get_task(self) -> Task:
        """
        Calculate which job has fewer resource
        """
        weighted_resouces = []
        for job_name in self.jobs:
            resource_possesion = self.cal_resource_possesion(job_name)
            weighted_resouces.append((resource_possesion, job_name))
        
        weighted_resouces.sort()
        for _, job_name in weighted_resouces:
            task = self.jobs[job_name].get_task()
            if task:
                # logging.debug("Have task in job tracker")
                return task
        # logging.debug("No task in job tracker")
        return None        

    def remove_resource(self, resource: Node):
        if resource not in self.resource_dists:
            return
        job_name = self.resource_dists[resource]
        if job_name in self.resource_nums:
            self.resource_nums[job_name] -= 1
        self.resource_dists.pop(resource)
        job = self.jobs[job_name]
        job.remove_resource(resource)
         
    def sumbit_job(self, job: Job):
        logging.debug("Submit job {}".format(job.job_name))
        job_name = job.job_name
        self.jobs[job_name] = job
        self.resource_nums[job_name] = 0

    def allocate_resource(self, task: Task, resource: Node):
        logging.debug("allocate resource")
        self.resource_nums[task.job_name] += 1
        self.resource_dists[resource] = task.job_name
        job = self.jobs[task.job_name]
        job.process_task(task, resource)
    
    def receive_result(self, work_node: Node, task: Task, result : List[str]):
        logging.debug("Receive result")
        logging.debug("Node: {}".format(work_node.hostname))
        logging.debug("Task: {}-{}".format(task.job_name, task.task_id))
        logging.debug("Result: {}".format(result))

        job = self.jobs[task.job_name]
        job.receive_result(work_node, task, result)
        logging.info("Number of resources allocated: {}".format(self.resource_nums))

        if work_node in self.resource_dists:
            self.resource_dists.pop(work_node)
        self.resource_nums[task.job_name] -= 1

        if job.is_completed:
            job.end_time = time.time()
            self.complete_listener(job)
            # self.jobs.pop(task.job_name)
            # self.resource_nums.pop(task.job_name)

            # show result
            # print(job.receive_result)
            

    def get_rate(self, job_name: str):
        return self.jobs[job_name].get_query_rate()

    def get_progress(self, job_name: str):
        return self.jobs[job_name].get_progress()

    def get_stats(self, job_name: str):
        return self.jobs[job_name].get_stats()

    def get_vms(self, job_name: str):
       return self.jobs[job_name].get_vms()
    
    def contain_job(self, job_name):
        return (job_name in self.jobs)
