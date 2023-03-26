import org.apache.spark.graphx.{Graph => Graph, VertexId,Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object ConnectedComponents {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)


    val edgesOfGraph: RDD[Edge[Long]]=sc.textFile(args(0)).map(lineGraph => { val (vertexGraph, adjacentGraph) = lineGraph.split(",").splitAt(1)
      (vertexGraph(0).toLong,adjacentGraph.toList.map(_.toLong))}).flatMap(e => e._2.map(v => (e._1,v))).map(node => Edge(node._1,node._2,node._1))

    val graphValues: Graph[Long, Long]=Graph.fromEdges(edgesOfGraph, "defValue").mapVertices((vid, _) => vid)


    val pregelFn = graphValues.pregel(Long.MaxValue,5)(
      (id, pastVal, nextVal) => math.min(pastVal, nextVal),
      tripletGraph => {
        if (tripletGraph.attr < tripletGraph.dstAttr)
        {
          Iterator((tripletGraph.dstId,tripletGraph.attr))
        }
        else if (tripletGraph.srcAttr < tripletGraph.attr)
        {
          Iterator((tripletGraph.dstId,tripletGraph.srcAttr))
        }
        else
        {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b)
    )

    val result = pregelFn.vertices.map(g => (g._2,1)).reduceByKey(_ + _).sortByKey().map(x => x._1.toString() + " " + x._2.toString())
    result.collect().foreach(println)

  }
}
