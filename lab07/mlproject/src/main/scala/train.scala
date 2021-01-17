import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object train extends App with Logging{

  def mkPipeline(df:DataFrame): Pipeline = {
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}

    val cv: CountVectorizer = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(df)

    val lr: LogisticRegression = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val i2s: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("category")
      //.setLabels(Array("prediction"))
      .setLabels(indexer.labels)

    new Pipeline()
      .setStages(Array(cv, indexer, lr, i2s))
  }

  val spark: SparkSession = SparkSession.builder
    .appName("ivan.shishkin_lab07")
    .getOrCreate()

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger(logName).setLevel(Level.INFO)

  logInfo(s"read train dataFrame ${ConfigUtils.getWeblogsFile(spark)}")
  val dfWeblogs: DataFrame = spark.read.json(ConfigUtils.getWeblogsFile(spark))

  val dfToTrain: DataFrame = dfWeblogs
    .withColumn("domains", col("visits.url"))
    .drop(col("visits"))

  logInfo("create spark pipeline")
  val pipeline = mkPipeline(dfToTrain)

  logInfo("train model with dfWebLogs")
  val model: PipelineModel = pipeline.fit(dfToTrain)

  logInfo(s"save model to ${ConfigUtils.getPipelineModel(spark)}")
  model.write.overwrite().save(ConfigUtils.getPipelineModel(spark))
}
