import json
import sys
from datetime import datetime, timedelta
import dateutil.parser
from graph import Graph

def median(l):
    '''
    Method for computing median based on vertex degrees.
    We could optimize this by using numpy package but wanted to provide
    solution based on std python libraries.
    '''
    #print "$$$$$ rolling medain: ", l
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
                    if payment[2] < (max_ts - timedelta(seconds=59))]
    return edgesToRemove

def writeOutput(data, f):
    '''
    Methid for writing out rolling mean to output.
    '''
    f.write(str(data) + "\n")


def process():
    '''
    Method for processing payment records with moving window of 1 minute.
    simulates real time straming by reading records one by one using python
    generator object to read lines from file.
    '''
    ### Initialize Graph
    g = Graph()

    ### Initialize activeNodes at current time
    activeRecords = []

    ### Initialize max_ts older date..
    max_ts = dateutil.parser.parse('1970-01-01T23:23:12Z')

    ### Process payment records....
    if len(sys.argv) == 0:
        print('Specify input and output arguments')    
    else:    
        f = open(sys.argv[1], 'r')
        wf = open(sys.argv[2], 'w')
    #with open('venmo_output/output.txt','w') as wf:
        #with open('venmo_input/venmo-trans.txt', 'r+') as f:
    for data in f:
                payment = json.loads(data)
                ts = dateutil.parser.parse(payment['created_time'])
                ### Keep track of current max processing record
                if ts > max_ts:
                    max_ts = ts

                ### Update Graph based on latest payment record
                if ts >= (max_ts - timedelta(seconds=59)):
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
