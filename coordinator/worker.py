from typing import Callable, List
import time
import logging
from coordinator.job import Task
from sdfs.file_system import FileSystem
from utility.config import DOWNLOAD_DIR
from ml.predict import predict


class Worker:
    def __init__(self, fs: FileSystem, finish_task: Callable[[Task, List[str]], None] ):
        self.fs = fs
        self.finish_task = finish_task
        self.is_working = False

    def work(self, task: Task) -> bool:
        self.is_working = True
        logging.info("Start time of job: {}".format(task.start_time))
        # download all the file
        # for filename in task.data_set:
        #     self.fs.download_file(filename)
        
        inference = []
        # time.sleep(10)
        # all files would be in download folder
        for filename in task.data_set:
            dir_path = 'data/imgnet_data/'
            # logging.error("Img: {}".format(filename))
            label, percentage = predict(dir_path + filename, task.model_name)
            inference.append(label)

        time.sleep(1)

        # fake inference
        # fake_inference = ["tmp" for _ in range(len(task.data_set))]

        self.finish_task(task, inference)
        self.is_working = False
        
