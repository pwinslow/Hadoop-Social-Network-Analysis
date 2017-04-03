# Import map-reduce modules
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol


# Define a class to handle nodes in a social network
class Node:
	
	'''
	Each node has 4 properties:
		ID: A unique number identifying a particular individual in the network
		connections: A list of IDs indicating the list of people this individual is connected to
		distance: A number indicating the number of connections between the individual and the a chosen person in the network
		color: An attribute indicating the status of a given node. A node can be white, gray, or black. If white (gray) {black}
		         then the node has yet to be explored (should now be explored) {has already been explored}.
	
	Methods:
	
		getinfo: Read in pipe-deliminated info about a node. Format is ID|CONNECTIONS|DISTANCE|COLOR where connections are in csv format.
		giveinfo: Return info in the same format that getinfo reads it in.
		
	'''
	
	def __init__(self):
		self.ID = ''
		self.connections = []
		self.distance = 9999
		self.color = 'WHITE'


	def getinfo(self, line):
		fields = line.split('|')
		if (len(fields) == 4):
			self.ID = fields[0]
			self.connections = fields[1].split(',')
			self.distance = int(fields[2])
			self.color = fields[3]

	def giveinfo(self):
		connections = ','.join(self.connections)
		return '|'.join( (self.ID, connections, str(self.distance), self.color) )
		
	
	
# Define a class to perform a breadth-first-search on the network using map-reduce
class MRBFSIteration(MRJob):

	'''
	Methods:

		configure_options: Set any configuration options for the map reduce job(s). In this case, we use add_passthrough_option to 
					  pass along the target person in the network.
		mapper: Perform the breadth-first-search.
		reducer: If the connection between the two specified nodes in the network is made in multiple different ways then this 
			   method chooses the shortest path between the two.
	
	'''
	
	# Normal output for MRJob is in json. Since we're running a breadth-first-search iteratively on a dataset we need to return 
	# data in the format it was entered in for the next iteration to run properly.
	INPUT_PROTOCOL = RawValueProtocol
	OUTPUT_PROTOCOL = RawValueProtocol
	
	
	def configure_options(self):
		super(MRBFSIteration, self).configure_options()
		self.add_passthrough_option('--target', help="ID of character we are searching for")


	def mapper(self, _, line):
		
		node = Node()
		node.getinfo(line)

		if (node.color == 'GRAY'):
			for connection in node.connections:
				
				temp_node = Node()
				temp_node.ID = connection
				temp_node.distance = int(node.distance) + 1
				temp_node.color = 'GRAY'
				if (self.options.target == connection):
					print temp_node.distance
				yield connection, temp_node.giveinfo()

			# We've processed this node, so color it black
			node.color = 'BLACK'

		# Emit the input node so we don't lose it.
		yield node.ID, node.giveinfo()


	def reducer(self, key, values):
		edges = []
		distance = 9999
		color = 'WHITE'

		for value in values:
			node = Node()
			node.getinfo(value)

			if (len(node.connections) > 0):
				edges.extend(node.connections)

			if (node.distance < distance):
				distance = node.distance

			if ( node.color == 'BLACK' ):
				color = 'BLACK'

			if ( node.color == 'GRAY' and color == 'WHITE' ):
				color = 'GRAY'

		node = Node()
		node.ID = key
		node.distance = distance
		node.color = color
		
		#There's a bug in mrjob for Windows where sorting fails with too much data. As a workaround, we're limiting the
		#number of edges to 500 here. You'd remove the [:500] if you were running this for real on a Linux cluster.
		node.connections = edges[:500]

		yield key, node.giveinfo()
		
		

if __name__ == '__main__':
	MRBFSIteration.run()
