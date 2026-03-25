package edu.metu.ceng790.hw1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.util.Try

case class Picture(fields: Array[String]) {
  val lat: Double = fields(11).toDouble
  val lon: Double = fields(10).toDouble
  val c: Country = Country.getCountryAt(lat, lon)
  val userTags: Array[String] = java.net.URLDecoder.decode(fields(8), "UTF-8").split(",")
  def hasTags: Boolean = {
    userTags.size > 0 && !(userTags.size == 1 && userTags(0).isEmpty())
  }
}

object Part2 {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
      spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      val originalFlickrMeta: RDD[String] = spark.sparkContext.textFile("data/flickrSample.txt")

      def printScope(title: String): Unit = {
        println("\n============================================================")
        println(title)
        println("============================================================")
      }

      // Question 1
      // We display the first 10 raw lines from the input RDD and then print
      // the total number of records in the dataset.
      printScope("Question 1 - First 10 lines from RDD[String] and total count")
      originalFlickrMeta.take(10).foreach(println)
      val totalRawElements: Long = originalFlickrMeta.count()
      println(s"Total number of elements in originalFlickrMeta: $totalRawElements")

      // Question 2
      val splitRecords = originalFlickrMeta.map(line => line.split("\t"))
      val pictures: RDD[Picture] = splitRecords.flatMap(fields => Try(Picture(fields)).toOption)
      val interestingPictures: RDD[Picture] = pictures.filter(pic => pic.c != null && pic.hasTags)

      printScope("Question 2 - First 10 elements from interesting RDD[Picture]")
      interestingPictures.take(10).foreach(println)

      // Question 3
      // We group interesting pictures by country.
      // The resulting RDD type is: RDD[(Country, Iterable[Picture])].
      val picturesByCountry = interestingPictures.groupBy(_.c)

      printScope("Question 3 - First country and its corresponding list of images")
      val firstCountryGroup = picturesByCountry.take(1)
      firstCountryGroup.foreach(println)

      // Question 4
      val tagsByCountry = picturesByCountry.map(item => {
        val country = item._1
        val pics = item._2
        val allTags = pics.flatMap(pic => pic.userTags).toList
        (country, allTags)
      })

      printScope("Question 4 - First 10 (Country, List[Tags]) pairs")
      tagsByCountry.take(10).foreach(println)

      // Question 5
      val tagFrequencyByCountry = tagsByCountry.map(item => {
        val country = item._1
        val tags = item._2
        
        val tagFrequencies = tags.groupBy(x => x).map(tagGroup => {
          val tag = tagGroup._1
          val occurrences = tagGroup._2
          (tag, occurrences.size)
        })
        
        (country, tagFrequencies)
      })

      printScope("Question 5 - First 10 (Country, Map[Tag -> Frequency]) pairs")
      tagFrequencyByCountry.take(10).foreach(println)

      spark.stop()


  }
}
