import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType, TimestampType}

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

    val kafkaTopic = "ivan_shishkin"
    val offset = "earliest"
    val df = spark
      .readStream
      .format("kafka")
      .option("subscribe", kafkaTopic)
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("maxOffsetsPerTrigger", "100")
      .option("startingOffsets", offset)
      .load
      .withColumn("value", from_json(col("value").cast(StringType), schema))


    val groupDF = df
      .withColumn("value.timestamp", col("value.timestamp") / lit(1000))
      .withColumn("ts", to_timestamp(col("value.timestamp") / lit(1000)))
      .withColumn("value.item_price", col("value.item_price").cast(DoubleType))
      .withWatermark("timestamp", "1 hour")
      .groupBy(window(col("ts"), "1 hour"))
      .agg(
        min("value.timestamp") / lit(1000) as "start_ts",
        max("value.timestamp") / lit(1000) as "end_ts",
        sum(when(col("value.event_type") === lit("buy"), col("value.item_price"))
          .otherwise(lit(0))) as "revenue",
        sum(when(col("value.event_type") === lit("buy"), lit(1))
          .otherwise(lit(0))) as "purchases",
        count("value.uid") as "visitors")
      .withColumn("aov", col("revenue") / col("purchases"))
      .select("start_ts", "end_ts", "revenue", "visitors", "purchases","aov")

    val dfWriter = groupDF
      .toJSON
      .withColumn("topic", lit("ivan_shishkin_lab04b_out"))
      .writeStream
      .outputMode("update")
      .format("kafka")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("kafka.bootstrap.servers", "10.0.0.5:6667")
      .option("checkpointLocation", s"chk/lab04b")
      .start


  }
}