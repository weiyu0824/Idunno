from typing import List
from threading import Lock
import copy
import logging

from group.node import Node

class ResourceTracker:
    def __init__(self):
        self.resource_lock = Lock()
        self.alive_nodes: List[Node] = []
        self.free_nodes: List[Node] = []
    
    def update_resource(self, resources: List[Node]) -> List[Node]:
        logging.info("Update resouces")
        fail_nodes: List[Node] = []
        join_nodes: List[Node] = []
        with self.resource_lock:

            # logging.debug("[Resources]")
            # for node in resources:
            #     logging.debug(node.hostname)
            

            for node in self.alive_nodes:
                # logging.debug("alive: {}".format(node.hostname))
                if node not in resources:
                    fail_nodes.append(node)
                    if node in self.free_nodes:
                        self.free_nodes.remove(node)
            for node in resources:
                # logging.debug("nodes: {}".format(node.hostname))
                if node not in self.alive_nodes:
                    join_nodes.append(node)

            # # Debug
            
            # logging.debug("[Free nodes]")
            # for node in self.free_nodes:
            #     logging.debug(node.hostname)
            # logging.debug("[Fail nodes]")
            # for node in fail_nodes:
            #     logging.debug(node.hostname)

            # logging.debug("[Alive nodes]")
            # for node in self.alive_nodes:
            #     logging.debug(node.hostname)

            # logging.debug("[Join nodes]")
            # for node in join_nodes:
            #     logging.debug(node.hostname)

            self.alive_nodes = copy.deepcopy(resources)
            self.free_nodes = self.free_nodes + join_nodes
        return fail_nodes

    def release_resources(self, node: Node):
        with self.resource_lock:
            if node in self.alive_nodes:
                self.free_nodes.append(node)

    def get_free_resource(self) -> Node:
        with self.resource_lock:
            # logging.debug("[Alive nodes]")
            # for node in self.alive_nodes:
            #     logging.debug(node.hostname)
            # logging.debug("[Free nodes]")
            # for node in self.free_nodes:
            #     logging.debug(node.hostname)

            if len(self.free_nodes) == 0:
                return None
            else:
                # logging.debug("Threre is free nodes")
                return self.free_nodes[-1]

    def num_resources(self):
        return len(self.alive_nodes)
    
    def occupy_resources(self, node: Node):
        with self.resource_lock:
            if node in self.alive_nodes and self.free_nodes:
                self.free_nodes.remove(node)