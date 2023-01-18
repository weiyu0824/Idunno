from typing import List
from group.node import Node, encode_node, decode_node

class MessageType:
    PING = 1
    ACK = 2
    JOIN = 3

class Message:
    def __init__(self, msg_type: int, membership_list: List[Node]):
        self.msg_type = msg_type
        self.membership_list = membership_list
        if not (msg_type == MessageType.PING 
            or msg_type == MessageType.ACK 
            or msg_type == MessageType.JOIN):
            print("[Error] This message is not valid")
            exit()

def encode_message(msg: Message) -> list:
    msg_list = [msg.msg_type] + [encode_node(member) for member in msg.membership_list]
    return msg_list

def decode_message(msg_list: list) -> Message:
    msg_type = msg_list[0]
    membership_list = [decode_node(node_list) for node_list in msg_list[1:]]
    return Message(msg_type, membership_list)
