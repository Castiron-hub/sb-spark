import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.Dataset

object agg extends App {

  override def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Stateful Streaming")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(
      StructField("event_type", StringType) ::
        StructField("category", StringType) ::
        StructField("item_id", StringType) ::
        StructField("item_price", StringType) ::
        StructField("uid", StringType) ::
        StructField("timestamp", LongType) :: Nil
    )

    def createConsoleSink(chkName: String, df: DataFrame) = {
      df
        .writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .option("checkpointLocation", s"chk/$chkName")
        .option("truncate", "false")
        .option("numRows", "1000")
    }
    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "ivan_shishkin",
      "startingOffsets" -> "earliest",
      "kafkaConsumer.pollTimeoutMs"-> "300000",
      "kafka.session.timeout.ms"-> "30000"
    )


    val sdf = spark.readStream.format("kafka").options(kafkaParams).load
      .select(
        from_json('value.cast(StringType) ,schemaFromClass).as("js"),
        'topic,
        'partition,
        'offset
      )
      .select(
        $"js.category",
        $"js.event_type",
        $"js.item_price",
        $"js.uid",
        to_timestamp(from_unixtime($"js.timestamp"/1000)).as("timestamp"),
        'topic,
        'partition,
        'offset
      )
      .withWatermark("timestamp", "1 hour")
      .groupBy(
        window(col("timestamp"), "1 hour", "1 hour")
      ).count

    def writeKafka[T](topic: String, data: Dataset[T]): Unit = {
      val kafkaParams = Map(
        "kafka.bootstrap.servers" -> "10.0.0.5:6667"
      )

      data.toJSON.withColumn("topic", lit(topic)).write.format("kafka").options(kafkaParams).save
    }
    writeKafka("ivan_shishkin_lab04b_out", sdf)

  }
}