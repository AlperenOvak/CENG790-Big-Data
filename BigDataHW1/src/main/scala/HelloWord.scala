import org.apache.spark.sql.SparkSession

object HelloWord {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TestApp")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.range(10)
    df.show()

    spark.stop()
  }
}