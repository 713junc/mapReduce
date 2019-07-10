from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class Node:
    def __init__(self): # Defult
        self.color = 'RED'
        self.value = ''
        self.connectedNodes = []
        self.distance = 999999

    # Input data: color | value | connectedNode | distance
    def extractData(self, line):
        colums = line.split('|')
        if (len(colums) == 4):
            self.color = colums[0]
            self.value = colums[1]
            self.connectedNodes = colums[2].split(',')
            self.distance = int(colums[3])

    def getData(self):
        connectedNodes = ','.join(self.connectedNodes)
        return '|'.join( (self.color, self.value, connectedNodes, str(self.distance)) )

class MapReduceBFS(MRJob):

    # Input and Output will be untouched
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_args(self):
        super(MapReduceBFS, self).configure_args()
        self.add_passthru_arg('--target', help="Target ID")

    def mapper(self, _, line):
        node = Node()
        node.extractData(line)

        if (node.color == 'BLUE'):
            for connectedNode in node.connectedNodes:
                newNode = Node()
                newNode.color = 'BLUE'
                newNode.value = connectedNode
                newNode.distance = int(node.distance) + 1
                if (self.options.target == connectedNode):
                    self.increment_counter('Degrees of Separation',
                    "Reached to Target ID: " + connectedNode +" with distance: " + str(newNode.distance), 1)
                yield connectedNode, newNode.getData()

            node.color = 'WHITE' #update the node has been processed
        yield node.value, node.getData()

    def reducer(self, key, data):
        color = 'RED'
        connections = []
        distance = 999999

        for line in data:
            node = Node()
            node.extractData(line)
            # color
            if(node.color == 'BLUE' and color == 'RED'):
                color = 'BLUE'
            if (node.color == 'WHITE'):
                color = 'WHITE'
            # connectedNodes
            if (len(node.connectedNodes) > 0):
                connections.extend(node.connectedNodes)
            # distance
            if (node.distance < distance):
                distance = node.distance

        resNode = Node()
        resNode.color = color
        resNode.value = key
        resNode.connectedNodes = connections
        resNode.distance = distance

        yield key, node.getData()


if __name__ == '__main__':
    MapReduceBFS.run()
