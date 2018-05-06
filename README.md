# Graph Analysis using SQL and GraphX

## Purpose

In this project, I imported a graph (stored as text files) into SQL and GraphX and answered some questions based on it. 

### A. Get the data

`data.zip` contains two files: amazon-data.txt and amazon-meta-clean.txt. The graph is a representation of Amazon website’s “Customers Who Bought This Item Also Bought” feature. If purchase of product i  frequently leads to purchase of product j, then the graph contains a directed edge from i to j. The data was collected in 2003 by crawling the Amazon website, and contains product metadata and review information about 548,552 different products (Books, music CDs, DVDs and VHS video tapes). More info is available here: 
1.	`amazon-data.txt`: each line has two vertex ids `i` and `j` representing an edge  `i -> j`.  [More info](https://snap.stanford.edu/data/amazon0302.html).
2.	`amazon-meta-clean.txt`: each line contains the following info: `vertex_id \t title \t type \t salesrank`. [More info](https://snap.stanford.edu/data/amazon-meta.html).

# C. Answer questions using SQL

Write queries to answer the following questions.

1.	The name of the most co-purchased product (if `i -> j` is the edge, then `j` is the co-purchased product here).
2.	The name of the most co-purchased DVD. 
3.	The average number of products that a product is co-purchased with. This is essentially the average in-degree of the given graph. 
4.	Count of all triplets of products containing the book `The Maine Coon Cat (Learning About Cats)`  that could form a ‘combo’ (say, for the purpose of a discount), such that the products in the triplet are co-purchased.  More specifically, if `a`, `b`, `c` form a triplet, then `a -> b`, `b->c`, `c->a` is true, and one of `a`, `b`, `c` needs to be the cat book specified above. 
5.	Find the length of the shortest path between the `Video` titled `Star Wars Animated Classics - Droids` (as source node) and the `Book` titled `The Maine Coon Cat (Learning About Cats)` (as destination node).

# D. Results

```
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
val v = sc.textFile("amazon-meta-clean.txt").map(x => (x.split("\t")(0).toLong,(x.split("\t")(1),x.split("\t")(2))) )
val e = sc.textFile("amazon-data.txt").map(x => Edge(x.split("\t")(0).toLong,x.split("\t")(1).toLong,0L))
val g = Graph(v,e)
```

#### Spark code for part 2

```
val sumEdges = g.edges.count()
val sumNonzeroVertices = g.degrees.filter(_._1 > 0).count()
sumEdges.toFloat / sumNonzeroVertices 
```

#### Spark code for part 3

```
val result = ShortestPaths.run(g, Seq(111))
val shortestPath = result.vertices.filter({case(v, _) => v == 1767}).first._2.get(111)
```

#### Spark code for part 4

```
val graph = GraphLoader.edgeListFile(sc, "amazon-data.txt", true)
val triCounts = graph.triangleCount().vertices
val ansNode = triCounts.reduce((a,b) =>  (a._1, a._2 + b._2))
ansNode._2/3
```
