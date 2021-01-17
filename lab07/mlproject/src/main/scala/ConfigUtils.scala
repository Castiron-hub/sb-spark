import org.apache.spark.sql.SparkSession

object ConfigUtils {

  private val pipeline_model: String = "/user/ivan.shishkin/pipeline_model"
  private val weblogs_file: String = "hdfs:///labs/laba07/weblogs_train_merged_labels.json"
  private val in_topic_name  = "ivan_shishkin"
  private val out_topic_name = "ivan_shishkin_lab04b_out"

  private val confPipelineModel: (String, String) = ("spark.mltrain.pipeline_model", pipeline_model)
  private val confWeblogsFile:  (String, String)  = ("spark.mltrain.weblogs_dir", weblogs_file)
  private val confInTopicName: (String, String)   = ("spark.mltrain.in_topic", in_topic_name)
  private val confOutTopicName:  (String, String)  = ("spark.mltrain.out_topic", out_topic_name)


  def getConf(spark: SparkSession, confName:(String, String) ): String = {
    spark.sparkContext.getConf.getOption(confName._1) match {
      case Some(x) =>  x
      case None => confName._2
    }
  }

  def getPipelineModel  (spark: SparkSession): String = getConf(spark,confPipelineModel)
  def getWeblogsFile (spark: SparkSession): String = getConf(spark, confWeblogsFile)
  def getInTopicName  (spark: SparkSession): String = getConf(spark,confInTopicName)
  def getOutTopicName  (spark: SparkSession): String = getConf(spark,confOutTopicName)
}