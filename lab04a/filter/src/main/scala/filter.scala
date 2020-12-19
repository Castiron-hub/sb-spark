import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr,col, date_format, from_json, from_unixtime, lit, struct, to_date}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import scala.reflect.io.Directory
import java.io.File
import java.net._


object filter {

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete) throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  def clean_target(path:String, conf:Configuration) = {
    val fs = FileSystem.get(conf)
    val outPutPath = new Path(path)
    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)
  }

  def upload_topic(spark:SparkSession, topic_name:String, offset: String, output_dir_prefix:String, filter: String):Unit = {
    var kafka_offset = "earliest"
    if (offset != "earliest") kafka_offset =  s""" { "$topic_name": { "0": $offset } } """
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> topic_name,
      "startingOffsets" ->  kafka_offset
    )

    val schema = StructType(Seq(
      StructField("event_type", StringType, true),
      StructField("category", StringType, true),
      StructField("item_id", StringType, true),
      StructField("item_price", IntegerType, true),
      StructField("uid", StringType, true),
      StructField("value", StringType, true),
      StructField("timestamp", LongType, true)
    ))

    val sdf = spark.read.format("kafka").options(kafkaParams).load
    val parsedSdf = sdf.select(col("value").cast("string").as("value")).withColumn("jsonData", from_json(col("value"), schema))
      .drop("value")
      .select(col("jsonData.*"))
      .withColumn("date",date_format((col("timestamp")/1000).cast("timestamp") - expr("INTERVAL 3 HOURS"),"yyyyMMdd"))

    parsedSdf.filter(col("event_type") === filter).write.format("json").partitionBy("date")
      .option("path", output_dir_prefix+"/"+filter).save
  }

  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .appName("ap_lab04")
      .enableHiveSupport()
      .getOrCreate()
    val topic = spark.conf.get("spark.filter.topic_name","lab04_input_data")
    val offset = spark.conf.get("spark.filter.offset","earliest")
    val prefix = spark.conf.get("spark.filter.output_dir_prefix")

    if(! (prefix contains "file:")) clean_target(prefix,spark.sparkContext.hadoopConfiguration)
    else {
      val uri = new URI(prefix)
      val f = new File(uri)
      deleteRecursively(f)
    }
    upload_topic(spark, topic, offset, prefix, "buy")
    upload_topic(spark, topic, offset, prefix, "view")
    println("check")
    spark.stop()
  }
}