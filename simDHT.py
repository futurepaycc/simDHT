#encoding: utf-8
import socket
from hashlib import sha1
from random import randint
from struct import unpack
from socket import inet_ntoa
from threading import Timer, Thread
from time import sleep

from bencode import bencode, bdecode

BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881)
] 
TID_LENGTH = 4
RE_JOIN_DHT_INTERVAL = 30

def entropy(length):
    return ''.join(chr(randint(0, 255)) for _ in xrange(length))

def random_id():
    hash = sha1()
    hash.update(entropy(20))
    return hash.digest()

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

def get_neighbor(target, end=10):
    return target[:end]+random_id()[end:]


class DHT(Thread):
    def __init__(self, master, bind_ip, bind_port, max_node_qsize):
        Thread.__init__(self)
        self.setDaemon(True)

        self.master = master
        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.max_node_qsize = max_node_qsize
        self.table = KTable()

        self.ufd = socket.socket(socket.AF_INET, 
            socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.bind_ip, self.bind_port))

        timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT)

    def run(self):
        self.join_DHT()
        while True:
            try:
                (data, address) = self.ufd.recvfrom(65536)
                msg = bdecode(data)
                self.on_message(msg, address)
            except Exception:
                pass

    def on_message(self, msg, address):
        try:
            if msg["y"] == "r":
                if msg["r"].has_key("nodes"):
                    self.process_find_node_response(msg, address)

            elif msg["y"] == "q":
                if msg["q"] == "find_node":
                    self.process_find_node_request(msg, address)

                elif msg["q"] == "get_peers":
                    self.process_get_peers_request(msg, address)
        except KeyError:
            pass

    def send_krpc(self, msg, address):
        try:
            self.ufd.sendto(bencode(msg), address)
        except Exception:
            pass

    def send_find_node(self, address, nid=None):
        nid = get_neighbor(nid) if nid else self.table.nid
        tid = entropy(TID_LENGTH)
        msg = dict(
            t = tid,
            y = "q",
            q = "find_node",
            a = dict(id = nid, target = random_id())
        )
        self.send_krpc(msg, address)

    def join_DHT(self):
        for address in BOOTSTRAP_NODES: 
            self.send_find_node(address)

    def re_join_DHT(self):
        self.join_DHT()
        timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT) 

    def wander(self):
        while True:
            for node in list(set(self.table.nodes))[:self.max_node_qsize]:
                self.send_find_node((node.ip, node.port), node.nid)
            self.table.nodes = []
            sleep(1)

    def play_dead(self, tid, address):
        msg = dict(
            t = tid,
            y = "e",
            e = [202, "Server Error"]
        )
        self.send_krpc(msg, address)

    def process_find_node_response(self, msg, address):
        nodes = decode_nodes(msg["r"]["nodes"])
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20: continue
            if ip == self.bind_ip: continue
            self.table.put(KNode(nid, ip, port))

    def process_get_peers_request(self, msg, address):
        try:
            tid = msg["t"]
            infohash = msg["a"]["info_hash"]
            self.master.log(infohash, address)
            self.play_dead(tid, address)
        except KeyError:
            pass

    def process_find_node_request(self, msg, address):
        try:
            tid = msg["t"]
            target = msg["a"]["target"]
            self.master.log(target, address)
            self.play_dead(tid, address)
        except KeyError:
            pass

class KTable():
    def __init__(self):
        self.nid = random_id()
        self.nodes = []

    def put(self, node):
        self.nodes.append(node)


class KNode(object):
    def __init__(self, nid, ip=None, port=None):
        self.nid = nid
        self.ip = ip
        self.port = port

    def __eq__(self, node):
        return node.nid == self.nid

    def __hash__(self):
        return hash(self.nid)


#using example
class Master(object):
    def log(self, infohash, address=None):
        print "%s from %s:%s" % (infohash.encode("hex"), address[0], address[1])


if __name__ == "__main__":
    #max_node_qsize bigger, bandwith bigger, spped higher
    dht = DHT(Master(), "0.0.0.0", 6881, max_node_qsize=20)
    dht.start()
    dht.wander()