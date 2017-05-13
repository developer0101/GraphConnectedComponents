class Node:
	def __init__(self, id):
		self.id = id
		self.adj = []
		self.visited = False

		# For checking connectivity by label propogation
		self.label = id
		self.labelUpdate = True

	def __str__(self):
		return "{0},(l={1}):{2}".format(self.id, self.label, self.adj)
