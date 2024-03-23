# Superhero Degrees of Separation using Breadth First Search
# Find out how many superhero connections away from particular superhero

# Represent each line as a node with connections, a color, and a distance
# Color is initially white, and distance is initially 9999
"""For example,
    5983 1165 3836 4361 1282
    becomes
    (5983, (1165, 3836, 4361, 1282), 9999,  WHITE)
    Our initial condition is that a node is infinitely far away from the superhero
"""


# Boilerplate stuff:
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

# The characters we wish to find the degree of separation between:
startCharacterID = 5306  # SpiderMan
targetCharacterID = 14  # ADAM 3,031 (who?)
# targetCharacterID = 1
# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)


def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = "WHITE"
    distance = 9999

    if heroID == startCharacterID:
        color = "GRAY"
        distance = 0

    return (heroID, (connections, distance, color))


def createStartingRdd():
    inputFile = sc.textFile(
        r"C:\SparkCourse\pyspark-projects\SuperHero Socials\Marvel+Graph"
    )
    return inputFile.map(convertToBFS)


def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # If this node needs to be expanded...
    if color == "GRAY":
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = "GRAY"
            if targetCharacterID == connection:
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        # We've processed this node, so color it black
        color = "BLACK"

    # Emit the input node so we don't lose it.
    results.append((characterID, (connections, distance, color)))
    return results


"""
The bfsReduce function is a crucial component in the implementation of the Breadth-First Search (BFS) algorithm using Apache Spark. Its role is to combine or "reduce" the data for each character ID that has been processed through the bfsMap function. The reduction step is necessary because, during the map phase, each character or node in the graph might be processed multiple times, resulting in multiple records for the same character with different distances, colors, or connections. The bfsReduce function ensures that, for each character, we preserve the shortest distance found, the most comprehensive set of connections, and the "darkest" color according to the BFS algorithm's requirements.

Here's a detailed breakdown of how bfsReduce works:

Input: The function takes two data tuples (data1 and data2) as input. Each tuple represents the accumulated data for a character, including:

A list of connections (edges) to other characters.
The shortest distance from the starting character found so far.
The color of the node, which can be WHITE, GRAY, or BLACK, indicating whether the node is unvisited, currently being processed, or has been processed, respectively.
Combining Connections: The function first combines the connection lists from both inputs. This is important because, as the graph is explored, we might discover new connections for a character from different parts of the graph. By combining these lists, we ensure that we have a complete view of a character's connections.

Preserving the Shortest Distance: The function compares the distances in both inputs and preserves the shortest one. In BFS, the shortest distance to a node from the start node is of particular interest, as BFS is known for finding the shortest path in unweighted graphs.

Determining the "Darkest" Color: The function also determines the "darkest" color to preserve, based on a priority where BLACK > GRAY > WHITE. This step is crucial because:

A WHITE node indicates an unvisited node.
A GRAY node is one that has been discovered but whose connections haven't been fully explored.
A BLACK node has been fully processed, meaning all its connections have been explored.
By preserving the darkest color, the algorithm ensures that it correctly tracks the progress of the BFS traversal, avoiding unnecessary reprocessing of nodes that have already been fully explored.

Output: The function returns a tuple containing the combined list of connections, the shortest distance, and the darkest color. This output represents the reduced data for a character, ready to be used in the next iteration of the BFS algorithm or for final analysis.

In summary, the bfsReduce function is essential for ensuring the correctness and efficiency of the BFS algorithm in a distributed computing context like Apache Spark. By carefully combining and reducing data from the map phase, it helps maintain an accurate and concise representation of the state of the graph as it is being explored.
"""


def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if len(edges1) > 0:
        edges.extend(edges1)
    if len(edges2) > 0:
        edges.extend(edges2)

    # Preserve minimum distance
    if distance1 < distance:
        distance = distance1

    if distance2 < distance:
        distance = distance2

    # Preserve darkest color
    if color1 == "WHITE" and (color2 == "GRAY" or color2 == "BLACK"):
        color = color2

    if color1 == "GRAY" and color2 == "BLACK":
        color = color2

    if color2 == "WHITE" and (color1 == "GRAY" or color1 == "BLACK"):
        color = color1

    if color2 == "GRAY" and color1 == "BLACK":
        color = color1

    return (edges, distance, color)


# Main program here:
iterationRdd = createStartingRdd()

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration + 1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if hitCounter.value > 0:
        print(
            "Hit the target character! From "
            + str(hitCounter.value)
            + " different direction(s)."
        )
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
