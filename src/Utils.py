def BLOCK_LOW(world_rank, world_size, n):
	return ((world_rank) * (n) / (world_size))
def BLOCK_HIGH(world_rank, world_size, n):
	return (BLOCK_LOW((world_rank + 1), world_size,n) - 1)
def BLOCK_SIZE(world_rank, world_size, n):
	return (BLOCK_LOW((world_rank + 1), world_size, n) - BLOCK_LOW(world_rank, world_size, n))
def BLOCK_ID(id, world_size, n):
	return ((world_size * (id + 1) - 1) / n )

