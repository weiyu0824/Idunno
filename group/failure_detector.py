import os
import subprocess
import socket
import time
from threading import Lock
from group.node import Node, NodeState
from group.communication import Communication
from group.message import Message, MessageType, encode_message, decode_message
from threading import Thread
# from concurrent.futures import ThreadPoolExecutor
import logging
from typing import List, Dict, Callable
from utility.utils import serialize, deserialize, get_time
from utility.config import PORT, INTRODUCER_HOSTNAME, NUM_SUC





class FailureDetector:
    def __init__(self, hostname: str, timestamp: int, listeners: List[Callable[[List[Node]], None]]=[]):
        self.curr_node = Node(hostname, timestamp, NodeState.ALIVE, timestamp)
        self.membership_list_lock = Lock()
        self.membership_list : List[Node] = []
        self.dead_node_lookup : Dict[str, Node] = {}
        self.successors_lock = Lock()
        self.successors : List[Node] = [] # always 4 successors
        self.ping_records : List[bool] = [] # max=4
        self.listeners = listeners

        self.is_join = False
        self.comm = Communication(hostname, PORT, self.receive)
        communication_t = Thread(target=self.comm.run)
        communication_t.start()

    def register_listener(self, listeners: List[Callable[[List[Node]], None]]):
        for listener in listeners:
            self.listeners.append(listener)

    def join(self): # TODO: use "NEW" state to indicate if join or not
        if self.is_join:
            print('This server has already join!')
            return 
        # self.membership_list.append(self.curr_node)
        dead_node_list = list(self.dead_node_lookup.values())
        msg = Message(MessageType.JOIN, [self.curr_node] + dead_node_list)
        msg_bytes = serialize(encode_message(msg))
        self.comm.send(msg_bytes, (INTRODUCER_HOSTNAME, PORT))
        logging.debug("Send join message to introducer")
        
    def leave(self):
        ## TODO: make sure to close threads and socket
        os._exit(1)

    def ping_monitor(self):
        # executor = ThreadPoolExecutor(NUM_SUC)

        while True:
            self.successors_lock.acquire()
            ping_nodes = self.successors.copy()
            self.successors_lock.release()

            self.ping_records = [False for _ in range(len(ping_nodes))]
            for ping_id, ping_node in enumerate(ping_nodes):
                Thread(target=self.ping, args=(ping_id, (ping_node.hostname, PORT))).start()
                # executor.submit(self.ping, ping_id, (ping_node.hostname, PORT))
            time.sleep(2)

            is_change = False
            with self.membership_list_lock:
                for ping_record, ping_node in zip(self.ping_records, ping_nodes):
                    if ping_record == False:
                        pos = self.get_current_pos(ping_node)
                        if pos != -1:
                            self.membership_list[pos].state = NodeState.DEAD
                            self.dead_node_lookup[str(ping_node)] = self.membership_list[pos]
                            self.membership_list.pop(pos)   
                            is_change = True
                        # logging.debug('Find untrack node {}'.format(ping_node.hostname))                 
                if is_change:
                    self.on_update_membership()        
                self.update_successors()
   
    def ping(self, ping_id: int, ping_addr: tuple):
        # logging.debug("Ping Nodes({}) = {}".format(ping_id, ping_addr))
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(0) # no block
        for _ in range(2):
            dead_node_list = list(self.dead_node_lookup.values())
            msg = Message(MessageType.PING, self.membership_list + dead_node_list)
            msg_bytes = serialize(encode_message(msg))
            sock.sendto(msg_bytes, ping_addr)
            time.sleep(0.5)
            # listen ack
            try:
                data, _ = sock.recvfrom(2048)
                msg = decode_message(deserialize(data))
                self.update(msg)
                self.ping_records[ping_id] = True
                break
            except BlockingIOError:
                pass        

    def listen_ack(self, msg: Message):
        if self.is_join:
            logging.error('This server has already joined')
            return
        logging.info('Start join ...')
        self.update(msg)
        ping_monitor_t = Thread(target=self.ping_monitor)
        ping_monitor_t.start()
        self.is_join = True

    def listen_join(self, msg: Message, addr):
        # logging.debug('Listen a join message')
        # logging.debug('{} vs {}'.format(self.curr_node.hostname, INTRODUCER_HOSTNAME))
        if self.curr_node.hostname == INTRODUCER_HOSTNAME:
            # logging.debug('Receive join message from {}'.format(addr))
            self.update(msg)
            dead_node_list = list(self.dead_node_lookup.values())
            msg = Message(MessageType.ACK, self.membership_list + dead_node_list)
            self.comm.send(serialize(encode_message(msg)), addr)
            
    
    def listen_ping(self, msg: Message, addr: tuple):
        self.curr_node.latest_ack = get_time()
        self.update(msg)
        msg = Message(MessageType.ACK, [self.curr_node])
        self.comm.send(serialize(encode_message(msg)), addr)
    
    # TODO: check if curr node is dead?
    # TODO: other says that i am dead?
    # There may be a case that every says that i am dead but actually i am not,
    # Thus, I should leave and rejoin
    def update(self, msg: Message):
        # logging.debug('[(LISTEN PING) Other membership list]')
        # for member in msg.membership_list:
        #     logging.debug('{}, latest={}, state={}'.format(member.hostname, member.latest_ack, member.state))

        # Trick:
        # If two timestamp is same, then the node should be marked as dead if either member is marked dead
        is_change = False
        with self.membership_list_lock:
            for member in msg.membership_list:
                member_name = str(member)
                pos = self.get_current_pos(member)
                if pos != -1:
                    my_member = self.membership_list[pos]
                    if member.state == NodeState.ALIVE:
                        if member.latest_ack > my_member.latest_ack:
                            self.membership_list[pos] = member
                    elif member.state == NodeState.DEAD:
                        if member.latest_ack >= my_member.latest_ack:
                            self.membership_list.pop(pos)
                            self.dead_node_lookup[member_name] = member
                            is_change = True

                elif member_name in self.dead_node_lookup:
                    dead_node = self.dead_node_lookup[member_name]
                    if member.state == NodeState.ALIVE:
                        if member.latest_ack > dead_node.latest_ack:
                            self.dead_node_lookup.pop(member_name)
                            new_pos = self.find_empty_pos(member)
                            self.membership_list.insert(new_pos, member)
                            is_change = True
                    elif member.state == NodeState.DEAD:
                        if member.latest_ack > dead_node.latest_ack:
                            self.dead_node_lookup[member_name] = member
                            
                else:
                    if member.state == NodeState.DEAD:
                        self.dead_node_lookup[member_name] = member
                    else:
                        new_pos = self.find_empty_pos(member)
                        self.membership_list.insert(new_pos, member)
                    is_change = True
            if is_change:
                self.on_update_membership()
            self.update_successors()

    def update_successors(self):
        # Update successors
        curr_pos = self.get_current_pos(self.curr_node)
        num_suc = min(len(self.membership_list) - 1, NUM_SUC)
        self.successors_lock.acquire()
        self.successors = []
        for i in range(num_suc):
            self.successors.append(self.membership_list[(curr_pos + i + 1) % len(self.membership_list)])
        self.successors_lock.release()

    # TODO: check if I can change to 'node in self.memebership_list'
    def get_current_pos(self, node: Node):
        node_name = str(node)
        for pos, member in enumerate(self.membership_list):
            member_name = str(member)
            if node_name == member_name:
                return pos
        return -1

    def find_empty_pos(self, node: Node):
        # TODO: Right now I use hash code to sort the list, there may be a better way
        for pos, member in enumerate(self.membership_list):
            if node.hash_code > member.hash_code:
                return pos
        return len(self.membership_list)

    def receive(self, data: bytes, addr):
        msg = decode_message(deserialize(data))

        if msg.msg_type == MessageType.PING:
            # logging.debug('Receive PING type message')
            self.listen_ping(msg, addr)
        elif msg.msg_type == MessageType.ACK:
            # logging.debug('Receive ACK type message')
            self.listen_ack(msg)
        elif msg.msg_type == MessageType.JOIN:
            # logging.debug('Receive JOIN type message')
            self.listen_join(msg, addr)
        else:
            # logging.debug('Receive NOTHING type message')
            pass
    
    # TODO: listen for rejoin
    def rejoin():
        pass
    
    # TODO: Right now those called this function should already hold the lock
    # Are their any better implmentation
    def on_update_membership(self):
        for listener in self.listeners:
            listener(self.membership_list)

def command_line(fd: FailureDetector):
    while True:
        cmd = input("Type a command (list_mem, list_self, join, leave): ")
        if cmd == "list_mem": #TODO iter
            for member in fd.membership_list:
                print(str(member))
        elif cmd == "list_self":
            print("{}".format(str(fd.curr_node)))
        elif cmd == "join":
            fd.join()
        elif cmd == "leave":
            fd.leave()
        elif cmd == "list_dead":
            for member in list(fd.dead_node_lookup.values()):
                print(member)
        else:
            print("This command is not valid")


if __name__ =="__main__":
    logging.basicConfig(filename='fd.log', level=logging.DEBUG)
    logging.info('[FD] Start Logging ...')

    hostname = subprocess.check_output(["hostname"]).decode('ascii')[:-1]
    timestamp = get_time()
    print(hostname)
    print(timestamp)

    fd = FailureDetector(hostname, timestamp)   

    # init command line
    command_line_t = Thread(target=command_line, args=[fd])
    command_line_t.start()
