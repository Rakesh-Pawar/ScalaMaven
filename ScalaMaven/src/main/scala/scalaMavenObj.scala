package scalaMavenObj
import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._
import  org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration

object scalaMavenObj{
  def main(args: Array[String]): Unit = {
    println("Scala with SBT ...")

    val spark = SparkSession.builder
      .appName("ScaalaSBT").master("local[1]").getOrCreate()
    println("Spark Object: "+ spark)

    // s3 credentials
    val accessKey = "AKIAZ23HUQHQOX74DHUF"
    val secretKey = "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"


    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", accessKey)

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", secretKey)

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    // S3 file path
    val inputFilePath = "s3a://ds-data-migration-bucket/incoming/1000_output.csv"

    //  read CSV file
    val data = spark.read.option("header", "true")
      .csv(inputFilePath)
    // Display csv file
    println("-"*20 + "Display CSV file" + "-"*20)
    println(data.show())

//  S3 file text file path
    val txtFile = "s3a://rakesh-pwar-s3-bucket/inputData/baankdata.txt"
//  read text file
    val txtData = spark.read.textFile(txtFile)
    println("-"*20 + "Display Text file" + "-"*20)
    txtData.collect().foreach(f=>{ println(f)})
// Stop Spark session object
    spark.stop()
  }
}
