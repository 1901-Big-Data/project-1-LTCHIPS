import plotly
import plotly.graph_objs as go
import sys

import re

def main():
    if(len(sys.argv) == 1):
        print("Usage: (input file) (output file) (graph name) (x-axis name) (y-axis name)")
        exit(0)
    if (sys.argv[1] == ""):
        print("Usage: (input file) (output file) (graph name) (x-axis name) (y-axis name)")
        exit(0)
    if (sys.argv[2] == ""):
        print("Usage: (input file) (output file) (graph name) (x-axis name) (y-axis name)")
        exit(0)

    inputFile = open(sys.argv[1], "r")
    title = ""
    xAxisTitle = ""
    yAxisTitle = ""

    if (len(sys.argv) >= 4):
        title = sys.argv[3]

    if (len(sys.argv) >= 6):
        xAxisTitle = sys.argv[4]
        yAxisTitle = sys.argv[5]
    layOut = {
        'xaxis' : { 'title' : xAxisTitle},
        'yaxis' : { 'title' : yAxisTitle},
        'title' : title
        }
    
    listKey = []
    listData = []
    print("Reading file...")
    for line in inputFile.readlines():
        splitLine = re.split('\t', line)
        listData.append(float(splitLine[1]))
        listKey.append(splitLine[0])
    
    print("Generating Graph...")
    bar1 = go.Bar(x=listKey,
                  y=listData)

    figure = go.Figure(data=[bar1],layout=layOut)
    
    plotly.offline.plot(figure, filename=sys.argv[2])
    print("Done")
    
    
    
    exit(1)




main()
