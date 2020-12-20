import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConverters._

object agg extends App {
  val spark = SparkSession.builder().appName("filter").getOrCreate()

  import spark.implicits._

  val topicName = "ivan_shishkin"

  val sdf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", topicName)
    .load()

  val parsedSdf = sdf
    .select(json_tuple('value.cast("string"), "category", "event_type", "item_id", "item_price", "timestamp", "uid"))
    .select('c0.alias("category"), 'c1.alias("event_type"), 'c2.alias("item_id"), 'c3.alias("item_price"), 'c4.alias("timestamp"), 'c5.alias("uid"))
    .withColumn("timestamp", (col("timestamp")/1000).cast("timestamp"))

  val dfForKafka = parsedSdf
    .groupBy(window('timestamp, "1 hour", "1 hour"))
    .agg(sum(when('event_type === "buy", 'item_price).otherwise(0)).alias("revenue"),
      sum(when('event_type === "buy", 1).otherwise(0)).alias("purchases"),
      sum(when('uid.isNotNull, 1).otherwise(0)).alias("visitors"))
    .withColumn("aov", 'revenue/'purchases)
    .withColumn("start_ts", unix_timestamp(col("window.start")))
    .withColumn("end_ts", unix_timestamp(col("window.end")))
    .select('start_ts, 'end_ts, 'revenue, 'visitors, 'purchases, 'aov)

  val columns = dfForKafka.columns.map((col))

  val dfAgg = dfForKafka.select((to_json(struct(columns:_*)).alias("value")))

  val sink = createKafkaSink(dfAgg, topicName)
  val sq = sink.start()
  sq.awaitTermination()

  def createKafkaSink(df: DataFrame, topicName: String) = {
    df.writeStream
      .format("kafka")
      .outputMode("update")
      .option("checkpointLocation", "chk/"+topicName)
      .option("kafka.bootstrap.servers", "10.0.0.5:6667")
      .option("topic", topicName + "_lab04b_out")
      .trigger(Trigger.ProcessingTime("5 seconds"))
  }
}
