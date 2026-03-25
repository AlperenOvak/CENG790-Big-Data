package edu.metu.ceng790.hw1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object Part1 {
  def main(args: Array[String]): Unit = {
    val  spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    def printScope(title: String): Unit = {
      println("\n============================================================")
      println(title)
      println("============================================================")
    }

      //   * Photo/video identifier
      //   * User NSID
      //   * User nickname
      //   * Date taken
      //   * Date uploaded
      //   * Capture device
      //   * Title
      //   * Description
      //   * User tags (comma-separated)
      //   * Machine tags (comma-separated)
      //   * Longitude
      //   * Latitude
      //   * Accuracy
      //   * Photo/video page URL
      //   * Photo/video download URL
      //   * License name
      //   * License URL
      //   * Photo/video server identifier
      //   * Photo/video farm identifier
      //   * Photo/video secret
      //   * Photo/video secret original
      //   * Photo/video extension original
      //   * Photos/video marker (0 = photo, 1 = video)

      val customSchemaFlickrMeta = StructType(Array(
        StructField("photo_id", LongType, true),
        StructField("user_id", StringType, true),
        StructField("user_nickname", StringType, true),
        StructField("date_taken", StringType, true),
        StructField("date_uploaded", StringType, true),
        StructField("device", StringType, true),
        StructField("title", StringType, true),
        StructField("description", StringType, true),
        StructField("user_tags", StringType, true),
        StructField("machine_tags", StringType, true),
        StructField("longitude", FloatType, false),
        StructField("latitude", FloatType, false),
        StructField("accuracy", StringType, true),
        StructField("url", StringType, true),
        StructField("download_url", StringType, true),
        StructField("license", StringType, true),
        StructField("license_url", StringType, true),
        StructField("server_id", StringType, true),
        StructField("farm_id", StringType, true),
        StructField("secret", StringType, true),
        StructField("secret_original", StringType, true),
        StructField("extension_original", StringType, true),
        StructField("marker", ByteType, true)))

      val originalFlickrMeta = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .schema(customSchemaFlickrMeta)
        .load("data/flickrSample.txt")

    // Q1: Select identifier, GPS coordinates, and license type using Spark SQL.
    originalFlickrMeta.createOrReplaceTempView("flickrMeta")
    val selectPhotos = spark.sql(
      "SELECT photo_id, longitude, latitude, license " +
      "FROM flickrMeta"
    )
    printScope("Question 1 - Selected fields: identifier, GPS coordinates, license")
    selectPhotos.show()

    // Q2: Keep only interesting photos (non-null license and valid coordinates).
    val interestingPhotos = selectPhotos.filter(
      col("license").isNotNull &&
        col("latitude") =!= -1.0 &&
        col("longitude") =!= -1.0
    )

    printScope("Question 2 - Interesting pictures DataFrame (license != null, coords != -1.0)")

    // Q3 + Q4: Show data and execution plan for interesting photos.
    printScope("Question 3 - Execution plan for interesting pictures DataFrame")
    interestingPhotos.explain()
    printScope("Question 4 - Display interesting pictures")
    interestingPhotos.show()
    

    // Q5a: Load license properties and keep only NonDerivative licenses.

    val licenseDF = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("data/FlickrLicense.txt")

    printScope("Question 5(a) - License file content")
    licenseDF.show()
    printScope("Question 5(a) - License file schema")
    licenseDF.printSchema()

    val nonDerivLicenses = licenseDF.filter(
      col("NonDerivative")===1
    )

    printScope("Question 5(a) - Filtered NonDerivative licenses")
    nonDerivLicenses.show()


    // Q5a: Join interesting photos with NonDerivative licenses (before cache).
    val finalDF_beforeCache = interestingPhotos.join(
      nonDerivLicenses,
      interestingPhotos("license") === nonDerivLicenses("Name")
    )
    printScope("Question 5(a) - Join execution plan before cache")
    finalDF_beforeCache.explain()
    printScope("Question 5(a) - Join results before cache")
    finalDF_beforeCache.show()

    // Q5b: Cache reused DataFrame, then inspect plan again.
    interestingPhotos.cache()

    // AFTER CACHE - show join execution plan with cached data
    val finalDF_afterCache = interestingPhotos.join(
      nonDerivLicenses,
      interestingPhotos("license") === nonDerivLicenses("Name")
    )
    printScope("Question 5(b) - Join execution plan after caching interesting pictures")
    finalDF_afterCache.explain()

    printScope("Question 5(b) - Join results after cache")
    finalDF_afterCache.show()



    // Q5c: Save final result as CSV with header.
    finalDF_afterCache.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/interesting_nonderivative_photos")
    printScope("Question 5(c) - CSV written with header to output/interesting_nonderivative_photos")
  }
}
