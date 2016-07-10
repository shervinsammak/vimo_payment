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

            ### Check boundary condition if there are no neighbhors
            ### delete the vertext itself
            if len(_f.connectedTo) == 0:
                self.vertexList.pop(edge[0], None)

            if len(_t.connectedTo) == 0:
                self.vertexList.pop(edge[1], None)

    def __iter__(self):
        return iter(self.vertexList.values())
