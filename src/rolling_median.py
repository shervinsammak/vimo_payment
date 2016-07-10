import json
import sys
from datetime import datetime, timedelta
import dateutil.parser

class Graph:
    '''
    Abstract data structure for representing graph.
    '''
    def __init__(self):
        self.vertexList = {}
        self.numVertices = 0

    def addVertex(self, key):
        if key not in self.vertexList:
            self.numVertices = self.numVertices+1
            newVertex = Vertex(key)
            self.vertexList[key] = newVertex
            return newVertex

    def getVertex(self, n):
        if n in self.vertexList:
            return self.vertexList[n]
        else:
            return None

    def addEdge(self, f,t, cost=0):
        if f not in self.vertexList:
            nv = self.addVertex(f)
        if t not in self.vertexList:
            nv = self.addVertex(t)
        ### Add the edge both nodes/vertices to represent undirected graph...
        self.vertexList[f].addNeighbor(self.vertexList[t], cost)
        self.vertexList[t].addNeighbor(self.vertexList[f], cost)

    def getVertices(self):
        return self.vertexList.keys()

    def getVerticesDegrees(self):
        return [len(self.getVertex(node).getConnections()) for node in self.getVertices()]

    def removeEdges(self, edgesToRemove):
        for edge in edgesToRemove:
            if self.getVertex(edge[0]) != None and self.getVertex(edge[1]) != None:
                _f = self.getVertex(edge[0]) ## From
                _t = self.getVertex(edge[1]) ## to
                _f.removeNeighbor(self.vertexList[edge[1]])
                _t.removeNeighbor(self.vertexList[edge[0]])
            ### Check boundary condition if there are no neighbors
            ### delete the vertex itself
            if len(_f.connectedTo) == 0:
                self.vertexList.pop(edge[0], None)

            if len(_t.connectedTo) == 0:
                self.vertexList.pop(edge[1], None)

    def __iter__(self):
        return iter(self.vertexList.values())

class Vertex:
    '''
    Abstract Data Structure to capture a Node/Vertex in a graph.
    '''
    def __init__(self, key):
        self.id = key
        self.connectedTo = {}

    def addNeighbor(self, nbr, weight=0):
        self.connectedTo[nbr] = weight

    def removeNeighbor(self, nbr):
        self.connectedTo.pop(nbr, None)

    def __str__(self):
        return str(self.id) + ' connectedTo: ' + str([x.id for x in self.connectedTo])

    def getConnections(self):
        return self.connectedTo.keys()

    def getId(self):
        return self.id

    def getWeight(self, nbr):
        return self.connectedTo[nbr]


def median(l):

    sortedLst = sorted(l)
    lstLen = len(l)
    index = (lstLen - 1) // 2
    if (lstLen % 2):
        return "{0:.2f}".format(sortedLst[index])
    else:
        return "{0:.2f}".format((sortedLst[index] + sortedLst[index + 1])/2.0)

def invalidateEdges(activeRecords, max_ts):
    '''
    Utility method for identifying invalid Vertices in the graph which are
    expired.
    '''
    edgesToRemove = \
      [(payment[0],payment[1]) for payment in activeRecords \
                    if payment[2] < (max_ts - timedelta(seconds=60))]
    return edgesToRemove

def writeOutput(data, f):
    '''
    Method for writing out rolling mean to output.
    '''
    f.write(str(data) + "\n")


def process():
    '''
    Method for processing payment records with moving window of 1 minute.
    simulates real time streaming by reading records one by one using python
    generator object to read lines from file.
    '''
    ### Initialize Graph
    g = Graph()
    ### Initialize activeNodes at current time
    activeRecords = []
    ### Initialize max_ts older date..
    max_ts = dateutil.parser.parse('1989-07-10T13:20:02Z')
    ### Process payment records....
    if len(sys.argv) == 0:
        print('Specify input and output arguments')    
    else:    
        f = open(sys.argv[1], 'r')
        wf = open(sys.argv[2], 'w')
        
    for data in f:
                payment = json.loads(data)
                ts = dateutil.parser.parse(payment['created_time'])
                ### Keep track of current max processing record
                if ts > max_ts:
                    max_ts = ts
                ### Update Graph based on latest payment record
                if ts >= (max_ts - timedelta(seconds=60)):
                    activeRecords.append((payment['actor'],payment['target'],ts))
                    ### Remove edges based on expiry of 1 minute windows
                    edgesToRemove = invalidateEdges(activeRecords, max_ts)
                    if len(edgesToRemove) >= 1:
                        g.removeEdges(edgesToRemove)
                        #### Update activeRecord list by removing the edges...
                        activeRecords = \
                           [record for record in activeRecords \
                              if (record[0],record[1]) not in edgesToRemove]
                    ### Update latest paymenent
                    g.addVertex(payment['actor'])
                    g.addEdge(payment['actor'], payment['target'])
                    writeOutput(median(g.getVerticesDegrees()), wf)
                else:
                    ### Write out median even in cases of out of order record
                    writeOutput(median(g.getVerticesDegrees()), wf)

if __name__ == '__main__':
    process()
