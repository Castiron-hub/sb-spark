import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object test extends App with Logging {

  import org.apache.spark.sql.types._

  val sch = StructType (
    StructField("timestamp", LongType) ::
      StructField("url", StringType) :: Nil)
  val schema = StructType (Seq(
    StructField("uid", StringType),
    StructField("visits", ArrayType (sch)) )
  )

  val spark: SparkSession = SparkSession.builder
    .appName("ivan.shishkin_lab07")
    .getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger(logName).setLevel(Level.INFO)

  val inputTopic: String = ConfigUtils.getInTopicName(spark)

  logInfo(s"start reading kafka stream topic ${inputTopic}")
  val inputStream: DataFrame = spark
    .readStream
    .format("kafka")
    .option("startingOffsets", "earliest")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", inputTopic)
    .load()

  val initial: DataFrame = inputStream.selectExpr("CAST(value AS STRING)").toDF("value")

  val parsed: DataFrame = initial
    .select(from_json(col("value"), schema).alias("tmp"))
    .select("*")
    .withColumn("uid", col("tmp.uid"))
    .withColumn("domains", col("tmp.visits.url"))
    .drop("tmp")

  val model: PipelineModel = PipelineModel.load(ConfigUtils.getPipelineModel(spark))
  val dfFinal: DataFrame = model.transform(parsed)
    .withColumnRenamed("label", "prediction")
    .withColumnRenamed("category","gender_age")
    .select("uid","gender_age")
    .toJSON.as("value")
    .select(col("value"))

  val outTopic: String = ConfigUtils.getOutTopicName(spark)

  val snk: DataStreamWriter[Row] = dfFinal
    .writeStream
    .outputMode("update")
    .format("kafka")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("checkpointLocation", s"chk/$outTopic")
    .option("topic", outTopic)

  val a: StreamingQuery = snk.start

  a.awaitTermination(50000)

  }

