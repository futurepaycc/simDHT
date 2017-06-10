#!/usr/bin/env python
# encoding: utf-8
'''
vs code 折叠技巧:
c+k c+0 全局折叠(数字0)
c+k c+j 全局展开

c+k c+[/] 局部(涵子块折叠,展开)
'''

import socket
from hashlib import sha1
from random import randint
from struct import unpack
from socket import inet_ntoa
from threading import Timer, Thread
from time import sleep
from collections import deque

from bencode import bencode, bdecode

BOOTSTRAP_NODES = (
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
)
TID_LENGTH = 2
RE_JOIN_DHT_INTERVAL = 3
TOKEN_LENGTH = 2


def entropy(length):
    return "".join(chr(randint(0, 255)) for _ in xrange(length))


def random_id():
    h = sha1()
    h.update(entropy(20))
    return h.digest()


def decode_nodes(nodes):
    n = []
    length = len(nodes)
    if (length % 26) != 0:
        return n

    for i in range(0, length, 26):
        nid = nodes[i:i+20]
        ip = inet_ntoa(nodes[i+20:i+24])
        port = unpack("!H", nodes[i+24:i+26])[0]
        n.append((nid, ip, port))

    return n


def timer(t, f):
    Timer(t, f).start()

#######################################工具方法去结束,业务方法区开始#####################################

#get_neighbor target: 目标node id, nid,自身node id
#返回邻居节点的简化实现
def get_neighbor(target, nid, end=10):
    return target[:end]+nid[end:] #目标的前10字节+自己的后十字节


#######################################业务方法结束,业务对象区开始########################################3

class KNode(object):   #节点对象

    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHTClient(Thread): #继承自线程

    def __init__(self, max_node_qsize):
        Thread.__init__(self)
        self.setDaemon(True)
        self.max_node_qsize = max_node_qsize
        self.nid = random_id()
        self.nodes = deque(maxlen=max_node_qsize)

    def send_krpc(self, msg, address): #rpc消息规范: http://www.bittorrent.org/beps/bep_0005.html
        try:
            self.ufd.sendto(bencode(msg), address)
        except Exception:
            pass
    
    '''
    用法1: 启动加入dht网络(相当于查找自己??)
    用法2: 查找某一节点nid
    ''' 
    def send_find_node(self, address, nid=None): #发出查找节点的请求(节点由作为server监听而来)
        nid = get_neighbor(nid, self.nid) if nid else self.nid #查找邻居节点(一个三元赋值表达式)
        tid = entropy(TID_LENGTH)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": nid,
                "target": random_id()
            }
        }
        self.send_krpc(msg, address)

    def join_DHT(self):
        for address in BOOTSTRAP_NODES:
            self.send_find_node(address) #加入时,不传nid

    def re_join_DHT(self):
        if len(self.nodes) == 0:
            self.join_DHT()
        timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT) #每隔一段时间,检查节点列表,如果为空,重新加入dht网络

    def auto_send_find_node(self):  #2,作为客户端时的入口(消费者主循环)
        wait = 1.0 / self.max_node_qsize #max_node_qsize 又实例化是传入200,这里的意思是一秒内启动完
        while True:
            try:
                node = self.nodes.popleft() #如果为空链表,会抛出异常(但被下面忽略了),直到有值为止(作为服务器是监听得到的节点信息)
                self.send_find_node((node.ip, node.port), node.nid)
            except IndexError:
                pass
            sleep(wait)

    # 处理find_peer 请求的响应,就是讲查到的节点放入self.nodes中
    def process_find_node_response(self, msg, address):
        nodes = decode_nodes(msg["r"]["nodes"])
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20: continue
            if ip == self.bind_ip: continue
            if port < 1 or port > 65535: continue
            n = KNode(nid, ip, port)
            self.nodes.append(n)


class DHTServer(DHTClient): #继承语法,不是构造参数

    def __init__(self, master, bind_ip, bind_port, max_node_qsize): #构造函数
        DHTClient.__init__(self, max_node_qsize)

        self.master = master
        self.bind_ip = bind_ip
        self.bind_port = bind_port

        self.process_request_actions = {
            "get_peers": self.on_get_peers_request,
            "announce_peer": self.on_announce_peer_request,
        }

        self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.bind_ip, self.bind_port))

        timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT)


    def run(self):  #1,作为服务器端时,程序运行入口(是个线程)
        self.re_join_DHT()
        while True: # 作为服务器时的主循环
            try:
                (data, address) = self.ufd.recvfrom(65536) #从socket读取数据,参数是缓冲区大小
                msg = bdecode(data)
                self.on_message(msg, address)
            except Exception:
                pass

    def on_message(self, msg, address):
        try:
            if msg["y"] == "r": 
                if msg["r"].has_key("nodes"): #find_node 查询的响应消息
                    self.process_find_node_response(msg, address)
            elif msg["y"] == "q": # 查询消息
                try:
                    self.process_request_actions[msg["q"]](msg, address)
                except KeyError:
                    self.play_dead(msg, address)
        except KeyError:
            pass

    def on_get_peers_request(self, msg, address): #响应对方节点的 查询资源 请求
        try:
            infohash = msg["a"]["info_hash"]
            tid = msg["t"]
            nid = msg["a"]["id"]
            token = infohash[:TOKEN_LENGTH]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(infohash, self.nid),
                    "nodes": "",
                    "token": token
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def on_announce_peer_request(self, msg, address): #响应关联节点的 发布资源 通知
        try:
            infohash = msg["a"]["info_hash"]
            token = msg["a"]["token"]
            nid = msg["a"]["id"]
            tid = msg["t"]

            if infohash[:TOKEN_LENGTH] == token:
                if msg["a"].has_key("implied_port") and msg["a"]["implied_port"] != 0:
                    port = address[1]
                else:
                    port = msg["a"]["port"]
                    if port < 1 or port > 65535: return
                self.master.log(infohash, (address[0], port)) #打印新发布资源信息
        except Exception:
            pass
        finally:
            self.ok(msg, address)

    def play_dead(self, msg, address):
        try:
            tid = msg["t"]
            msg = {
                "t": tid,
                "y": "e",
                "e": [202, "Server Error"]
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def ok(self, msg, address):
        try:
            tid = msg["t"]
            nid = msg["a"]["id"]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(nid, self.nid)
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass


class Master(object):

    def log(self, infohash, address=None):
        print "%s from %s:%s" % (
            infohash.encode("hex"), address[0], address[1]
        )


# using example
if __name__ == "__main__":
    # max_node_qsize bigger, bandwith bigger, speed higher
    dht = DHTServer(Master(), "0.0.0.0", 6881, max_node_qsize=200)
    dht.start() #启动server类run方法
    dht.auto_send_find_node()
