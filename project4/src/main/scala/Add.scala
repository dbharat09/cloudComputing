import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Add {
  val rows = 100
  val columns = 100

  case class Block ( data: Array[Double] ) {
    override
    def toString (): String = {
      var string = "\n"
      for ( p <- 0 until rows) {
        for ( q <- 0 until columns)
          string += "\t%.3f".format(data(p*rows+q))
        string += "\n"
      }
      string
    }
  }
  def toBlock ( triples: List[(Int,Int,Double)] ): Block = {
    var blockArray:Array[Double] = new Array[Double](rows*columns)
    triples.foreach(triple=> {
      blockArray(triple._1*rows+triple._2) = triple._3
    })

    new Block(blockArray)

  }
  def blockAdd ( m_block: Block, n_block: Block ): Block = {
    var blockArray:Array[Double] = new Array[Double](rows*columns)
    for ( p <- 0 until rows) {
      for ( q <- 0 until columns)
        blockArray(p*rows+q) = m_block.data(p*rows+q) + n_block.data(p*rows+q)
    }
    new Block(blockArray)

  }

  def createBlockMatrix ( scan: SparkContext, file: String ): RDD[((Int,Int),Block)] = {
    /* ... */
    scan.textFile(file).map( line => { val matrixValue = line.split(",")
      ((matrixValue(0).toInt/rows, matrixValue(1).toInt/columns), (matrixValue(0).toInt%rows, matrixValue(1).toInt%columns, matrixValue(2).toDouble)) } )
      .groupByKey().map{ case (k,value) => (k, toBlock(value.toList)) }
  }

  def main ( args: Array[String] ) {
    /* ... */
    val conf = new SparkConf().setAppName("Matrix_Addition")
    val sc = new SparkContext(conf)

    val mMatrix= createBlockMatrix(sc , args(0))
    val nMatrix=  createBlockMatrix(sc , args(1))

    val outputMatrix = mMatrix.join(nMatrix).map{ case(k, (mBlock, nBlock)) => (k, blockAdd(mBlock, nBlock)) }
    outputMatrix.saveAsTextFile(args(2))
    sc.stop()

  }
}