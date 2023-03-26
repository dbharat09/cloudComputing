import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

case class Pxn_Vertex(nodeID: Long, group: Long, adjacent: List[Long])

object Graph {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Connected Components of Graph")
    val sc = new SparkContext(conf)

    var graph = sc.textFile(args(0)).map(line => {

      val inputLine = line.split(",")

      (inputLine(0).toLong, inputLine(0).toLong, inputLine.slice(1, inputLine.length).toList.map(_.toLong))
    })

    for (i <- 1 to 5) {


      graph = graph.flatMap(grp_value => grp_value._3.flatMap(inputLine => Seq((inputLine, grp_value._2))) ++ Seq((grp_value._1, grp_value._2)))
        .reduceByKey((x_value, y_value) => x_value min y_value)
        .join(graph.map(grp_value => (grp_value._1, grp_value)))
        .map { case (nodeID, (min_value, grp_vertex)) => (nodeID, min_value, grp_vertex._3) }
    }

    // print the group sizes
    val grp_resul = graph.map(graph_value => (graph_value._2, 1)).reduceByKey(_+_).sortBy(_._1)
    grp_resul.map(x_value => {
      x_value._1 + "\t" + x_value._2
    }).collect().foreach(println)
    sc.stop()
  }
}
