import argparse
from BFSIteration import MRBFSIteration
import os
from cStringIO import StringIO
import sys


# Short class to capture stdout
class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout = self._stdout


# Method to transform ids to string valued names
def find_names(input_id, target_id):

	name_dic = {}

	with open('Marvel-Names.txt', 'r+') as f:
		
		for line in f:

			key_value = line.strip().split('"')
			key, value = int(key_value[0]), key_value[1:][0]
			name_dic[key] = value

	return (name_dic[input_id], name_dic[target_id])


# Method to get ids from cmd line at runtime
def GetArgs():
	
	parser = argparse.ArgumentParser()
	parser.add_argument('--input_id', type=str, required=True)
	parser.add_argument('--target_id', type=str, required=True)
	args = parser.parse_args()
	input_id = args.input_id
	target_id = args.target_id
	
	return (input_id, target_id)


# Method for transforming original dataset into required format for breadth-first search
def Initialize_BFS_Data(input_id, output_file):

	with open(output_file, 'w') as out:
	
		with open("Marvel-graph.txt") as f:
	
			for line in f:
				fields = line.split()
				heroID = fields[0]
				numConnections = len(fields) - 1
				connections = fields[-numConnections:]
	
				color = 'WHITE'
				distance = 9999
	
				if (heroID == input_id) :
					color = 'GRAY'
					distance = 0
	
				if (heroID != ''):
					edges = ','.join(connections)
					outStr = '|'.join((heroID, edges, str(distance), color))
					out.write(outStr)
					out.write("\n")
	
		f.close()
	
	out.close()



# Get input and target ids and specify a temporary data file to keep track of modifications made by BFS
input_id, target_id = GetArgs()
output_file = 'BFS-temp.txt'

# Initialize network data for BFS
Initialize_BFS_Data(input_id, output_file)

# Initialize instance of BFS class
arg_str = '--target {0} {1} --output=results'.format(target_id, output_file)
mr_job = MRBFSIteration(args=arg_str.split())


# Run map-reduce jobs in loop while capturing stdout 
with Capturing() as output:

	for i in xrange(20):

		# Run map-reduce
		with mr_job.make_runner() as runner:
			runner.run()

		# Replace social network dataset with map-reduce results and clean temporary files
		os.system('cat results/* > {0}'.format(output_file))
		os.system('rm -r results')

# Final clean-up
os.system('rm {0} *pyc'.format(output_file))

# Map-reduce prints degrees of separation for all possible paths. Print result for minimum path.
if output:
	degs = min([int(x) for x in output])
	input_name, target_name = find_names(int(input_id), int(target_id))
	print '{0} is {1} degrees of separation away from {2}'.format(input_name, degs, target_name)
else:
	print '{0} is more than {1} degrees of separation away from {2}'.format(input_name, 20, target_name)