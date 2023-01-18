from utility.config import HASH_M

class NodeState:
    ALIVE = 1
    DEAD = 2

class Node:
    def __init__(self, hostname: str, timestamp: int, state: NodeState, latest_ack: int):
        self.hostname = hostname
        self.timestamp = timestamp
        self.state = state
        self.latest_ack = latest_ack

        self.hash_code = hash((hostname, timestamp)) % HASH_M

    def __hash__(self):
        return hash((self.hostname, self.timestamp))

    def __eq__(self, other: object) -> bool:
        return str(self) == str(other)

    def __repr__(self):
        return self.hostname + "_" + str(self.timestamp) 

    def __str__(self):
    	return self.hostname + "_" + str(self.timestamp)

def encode_node(node: Node):
    return [node.hostname, node.timestamp, node.state, node.latest_ack]

def decode_node(node_list: list):
    return Node(node_list[0], node_list[1], node_list[2], node_list[3])