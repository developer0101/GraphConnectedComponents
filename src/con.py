# Parallel Label Propagation Algorithm for finding connected components of a graph
# University of Houston
# Authors: Reza Fathi
# Date: 05, 11, 2017
# Contact: rfathi@uh.edu

import sys
from mpi4py import MPI
from Process import Process
import time

if __name__=="__main__":
	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()
	worldSize = comm.Get_size()
	pname = MPI.Get_processor_name()
	print "Connectivity program started on {0}, rank={1}".format(pname, rank)
	process = Process(rank, worldSize, pname)

	filename = "test.txt"
	filename = "./" + filename

	process.loadGraph(filename)
	if rank==0:
		print "graph {0} is loaded".format(filename)

	# process.parallelBfs()
	s = time.time()
	process.labelPropogation()
	# process.graph.printGraph()
	# process.checkConnectivity()
	process.printStat()

	if rank == 0:
		print "The End!"
		print "Elapsed time=",time.time()-s

