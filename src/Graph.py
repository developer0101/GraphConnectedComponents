from Node import Node
class Graph:
	def __init__(self):
		self.nodes= {}
		self.n = 0
		self.m = 0

	def addEdge(self, src, des):
		if not self.nodes.has_key(src):
			self.nodes[src] = Node(src)
		self.nodes[src].adj.append(des)
		self.m = self.m + 1

	def localLabelPropogate(self):
		probNodes = dict.fromkeys(self.nodes.keys(), 1)
		while probNodes:
			for nodeid in probNodes.keys():
				node = self.nodes[nodeid]
				for nei in node.adj:
					if self.hash(nei) == self.hash(nodeid):
						neighbor = self.nodes[nei]
						if neighbor.label > node.label:
							neighbor.label = node.label
							neighbor.labelUpdate = True
							probNodes[nei] = 1
				del probNodes[nodeid]

	def printGraph(self):
		for node in self.nodes.values():
			print node

	def printCutEdges(self):
		for node in self.nodes.values():
			for nei in node.adj:
				if self.hash(nei) != self.hash(node.id):
					print "({0},{1})".format(node.id,nei)
