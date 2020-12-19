import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, from_json, col, from_unixtime}
import org.apache.spark.sql.{Dataset, DataFrame}
import java.io.File

object filter extends App {

  val kafkaAddress: String = "spark-master-1:6667"
  def writeKafka[T](topic: String, data: Dataset[T]): Unit = {
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> kafkaAddress
    )

    data.toJSON.withColumn("topic", lit(topic)).write.format("kafka").options(kafkaParams).save
  }

  def readKafka(kafkaAddress: String, topic: String, offset:String ): DataFrame = {
    val fullOffset = if (offset matches """\d+""") s""" { "$topic": { "0": $offset } } """ else offset
  val kafkaParams = Map(
        "kafka.bootstrap.servers" -> "spark-master-1:6667",
        "subscribe" -> "lab04_input_data",
        "startingOffsets" -> """earliest""",
        "endingOffsets" -> """latest"""
    )

    val df = spark.read.format("kafka").options(kafkaParams).load
    df
  }

  def deleteR(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteR(_))
    file.delete
  }

  val spark = SparkSession.builder
    .appName("lab04-a-sts")
    .getOrCreate()


  val struct = new StructType()
    .add("event_type", DataTypes.StringType)
    .add("category", DataTypes.StringType)
    .add("item_id", DataTypes.StringType)
    .add("item_price", DataTypes.FloatType)
    .add("uid", DataTypes.StringType)
    .add("timestamp", DataTypes.TimestampType)


  val topic_name = spark.conf.get("spark.filter.topic_name", "lab04_input_data")
  val offset = spark.conf.get("spark.filter.offset", "earliest")
  val output_dir_prefix = spark.conf.get("spark.filter.output_dir_prefix", "/user/ivan.shishkin/visits")
  val df = readKafka(kafkaAddress, topic_name, offset)

  val eventDf = df.select(from_json(col("value").cast("string"), struct).as("event"))

  val eventDfplain = eventDf.select(col("event.*"))
  val eventDfts = eventDfplain.select(col("*"), (col("timestamp").cast("long")/lit(1000) - lit(3) * lit(60) * lit(60)).alias("ts"))
  val eventDfDate = eventDfts.select(col("*"), from_unixtime(col("ts"), "yyyyMMdd").alias("date"))

  val eventList = List("buy", "view")
  eventList.foreach((i: String) => {
    deleteR(new File(s"/user/ivan.shishkin/visits/$i"))
    eventDfDate
      .filter(s"event_type = '$i'")
      .write
      .mode("overwrite")
      .partitionBy("date")
      .json(s"$output_dir_prefix/$i/")
  })

}