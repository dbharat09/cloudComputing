import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.split

object Netflix {
  def main ( args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Netflix")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import org.apache.spark.sql.functions.col

    val m1 = spark.read.option("delimiter", ";").csv(args(0))

    val m2 = m1.filter(col("_c0").contains(","))

    val df = m2.select(functions.split(col("_c0"),",").getItem(0).as("userID"),
      split(col("_c0"),",").getItem(1)as("rating"),
      split(col("_c0"),",").getItem(2)as("date")).drop("_c0")

    df.createOrReplaceTempView("userAndRating")

    val avgUserRatingDF = spark.sql("SELECT userID, substring(AVG(rating),0, instr(AVG(rating),'.')+1) as avgRating  FROM userAndRating group by userID")
    avgUserRatingDF.createOrReplaceTempView("avgRatingAndCount")

    val avgRatingANdCountDF = spark.sql("SELECT avgRating as rating, COUNT(avgRating) as countRating FROM avgRatingAndCount group by avgRating order by avgRating")
    avgRatingANdCountDF.show(41)

  }

}
