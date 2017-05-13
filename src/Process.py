import sys
from mpi4py import MPI
from Graph import Graph
from Queue import Queue
maxbufSize = 1000
comm = MPI.COMM_WORLD

# from enum import Eunum
# class ControlMessages(Enum):
# 	FORWARD_SIGNAL = -1
# 	ACK = -2

class Process:
    def __init__(self, rank, worldSize, name):
        self.rank = rank
        self.worldSize = worldSize
        self.name = name
        self.graph = Graph()
        self.graph.hash = self.hash

    def hash(self, nodeid):
        return nodeid % self.worldSize

    def addtoProcess(self, buf, p, src, des):
        buf[p].append((src, des))
        if len(buf[p]) >= maxbufSize:
            comm.send(buf[p], p, tag = 0)
            buf[p] = []

    def loadGraph(self, filename):
        if self.rank == 0:
            buf = [[] for _ in range(self.worldSize)]
            with open(filename,"r") as f:
                for line in f:
                    if "#" in line:
                        continue
                    verticies = line.replace("\r","").replace("\n","").split("\t")
                    if len(verticies) < 2:
                        continue

                    # verticies = line.split("\t")
                    src = int(verticies[0])
                    del verticies[0]
                    srcloc = self.hash(src)

                    for vertex in verticies:
                        des = int(vertex)
                        desloc = self.hash(des)
                        if srcloc == self.rank:
                            self.graph.addEdge(src, des)
                        else:
                            self.addtoProcess(buf, srcloc, src, des)

                        if desloc == self.rank:
                            self.graph.addEdge(des, src)
                        else:
                            self.addtoProcess(buf, desloc, des, src)

            for p in xrange(1, self.worldSize):
                if len(buf[p])>0:
                    comm.send(buf[p], p, tag = 0)
                comm.send([], p, tag = 0)
        else:
            # not Masters
            while True:
                edges = comm.recv(source = 0, tag= 0)
                if len(edges) == 0:
                    break
                for edge in edges:
                    src, des, desloc = edge[0], edge[1], self.hash(edge[1])
                    self.graph.addEdge(src, des)
                    if desloc == self.rank:
                        self.graph.addEdge(des, src)
        ln = len(self.graph.nodes)
        self.graph.n = comm.allreduce(ln, op=MPI.SUM)
        # self.graph.printGraph()

    def printStat(self):
        labels = {}
        for node in self.graph.nodes.values():
            labels[node.label] = labels.get(node.label, 0) + 1

        labels = comm.gather(labels.items(), root = 0)
        if self.rank == 0:
            dic = {}
            for lbls in labels:
                for l, c in lbls:
                    dic[l] = dic.get(l, 0) + c
            print "There are {0} connected subgraphs in graph".format(len(dic))
            # print dic.items()


    def parallelBfs(self):
        # TODO signal should be compeleted and bfs should same
        FORWARD_SIGNAL = -1
        localBfsCount, globalBfsCount = 0, 0
        if self.rank == 0:
            localBfsCount = self.bfs()
            if self.rank + 1 < self.worldSize:
                comm.send(FORWARD_SIGNAL, self.rank + 1, tag = 0)
        else:
            while True:
                data = comm.recv(source = MPI.ANY_SOURCE, tag= 0)
                if data == FORWARD_SIGNAL:
                    break
                else:
                    self.bfs(nodeid = data)
                    # Send Done feedback
            localBfsCount = self.bfs()
            if self.rank < self.worldSize -1:
                comm.send(signal, self.rank + 1, tag = 0)
        comm.Barrier()
        globalBfsCount = comm.reduce(localBfsCount, op=MPI.SUM, root=0) 
        if self.rank == 0:
            print "number of connected components=", globalBfsCount

    def bfs(self, nodeid = None):
        queue = Queue()
        count = 0
        if nodeid:
            prob = [nodeid]
        else:
            prob = self.graph.nodes.keys()
        for nodeid in prob:
            node = self.graph.nodes[nodeid]
            if not node.visited:
                count = count + 1
                queue.put(node.id)
                while not queue.empty():
                    headid = queue.get()
                    holdingprocess = self.hash(headid)
                    if holdingprocess == self.rank:
                        head = self.graph.nodes[headid]
                        if not head.visited:
                            head.visited = True
                            for nei in head.adj:
                                queue.put(nei)
                    else:
                        comm.send(headid, holdingprocess, tag = 0)
                        # TODO
                        # Wait for done if it is nodeid signal, work and reply for it
                        # data = comm.recv(source = MPI.ANY_SOURCE, tag= 0)
                        # while data != ControlMessages.ACK:
                        # 	self.bfs(data)
        return count

    def GHS(self):
        phase = 0
        leaves = {}

        print "GHS finished in {0} phases".format(phase)

    def exchangeLabels(self):
        globalUpdate = False
        messages = [[] for _ in range(self.worldSize)]
        recvmsgs = [[] for _ in range(self.worldSize)]
        for node in self.graph.nodes.values():
            if node.labelUpdate:
                for nei in node.adj:
                    holdingprocess = self.hash(nei)
                    if holdingprocess != self.rank:
                        # TODO break into pices, avoiding huge array size
                        messages[holdingprocess].append((nei,node.label))
                        
        for k in range(self.worldSize):
            if k != self.rank:
                recvmsgs[k] = comm.sendrecv(sendobj = messages[k], dest = k, source = k)

        for k in range(self.worldSize):
            if k != self.rank and recvmsgs[k]:
                for (rcv, label) in recvmsgs[k]:
                    node = self.graph.nodes[rcv]
                    if node.label > label:
                        node.label = label
                        globalUpdate = True

        return globalUpdate


    def labelPropogation(self):
        phase = 0
        update = True
        while update:
            phase = phase + 1
            if self.rank == 0:
                print "phase=",phase
            self.graph.localLabelPropogate()
            globalUpdate = self.exchangeLabels()
            update = comm.allreduce(globalUpdate, op = MPI.LOR)

        if self.rank==0:
        	print "phase=",phase
        	print "labelPropogation is done on machine ranked {0} and all".format(self.rank)

    def checkConnectivity(self):
        pass