import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object features extends App with Logging {


  def get_visits(spark: SparkSession, parquetName: String): DataFrame = {
    spark.read.parquet(parquetName)
      .select(col("uid"),explode(col("visits")).as("visit"))
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .withColumn("tmstmp", col("visit.timestamp")
        .divide(1000)
        .cast("timestamp"))
      .filter("host is not null")
      .filter("uid is not null")
  }

  def get_top1000(visitsDF:DataFrame): DataFrame = {
     visitsDF
      .select("domain").groupBy("domain")
      .agg(count(lit(1)).as("cnt"))
      .sort(desc("cnt"),asc("domain"))
      .limit(1000)//.show(false)
  }

  override def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("ivan_shishkin_lab06")
      //   .master("local")
      .getOrCreate()

    //spark.sparkContext.setLogLevel("ERROR")

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger(logName).setLevel(Level.INFO)

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    logInfo("create dfVisits")
    val dfVisits: DataFrame = get_visits(spark, "hdfs:///labs/laba03/weblogs.parquet")

    val top1000_DF: DataFrame = get_top1000(dfVisits).select("domain")

    val dfAnti: DataFrame = dfVisits.join(top1000_DF, Seq("domain"),"leftanti")
    val dfVfiltered: DataFrame = dfVisits.join(top1000_DF,Seq("domain"),"inner")
    val dfAntD: DataFrame = dfAnti.withColumn("dmn",lit("__ANTI__"))
    val dfPrepared: Dataset[Row] = dfVfiltered.select("uid","domain")
      .union(dfAntD.select("uid","dmn"))

    spark.conf.set("spark.sql.pivotMaxValues", 1010)
    logInfo("Pivot dfVisits && count visits for 1000-domain")
    val matrix1 = dfPrepared
      .groupBy("uid")
      .pivot("domain")
      .count.na.fill(0).drop("__ANTI__")

    logInfo("Make array of ordered columns by 1000-domains")
    import spark.implicits._
    val clmns: Array[Column] = top1000_DF
      .select("domain")
      .sort(asc("domain"))
      .as[String]
      .collect.map(x => col(s"`$x`"))

    logInfo("Create df: uid -> Array of count top 1000 domains visits")
    val mtrx: DataFrame = matrix1.select(col("uid"), array(clmns:_*).as("domain_features"))
    //val a = clmns.map(x => col(s"`$x`"))

    //val windowSpec  = Window.partitionBy("department").orderBy("salary")

/////Day && Hours

 //   val tdt = dfVisits
 //     //.withColumn("week_day_number", date_format(col("tmstmp"), "u"))
 //     .withColumn("hour", hour(col("tmstmp")))
 //     .withColumn("week_day_abb", date_format(col("tmstmp"), "E"))

    logInfo("make week days visits dataframe")
    val dfVisits_wd: DataFrame = dfVisits
      //.withColumn("week_day_number", date_format(col("tmstmp"), "u"))
 //     .withColumn("hour", hour(col("tmstmp")))
      .withColumn("week_day_abb",
        concat(lit("web_day_"),
          lower(date_format(col("tmstmp"), "E"))))
      .select("uid","week_day_abb")

    //Matrix with week days
    val matrix2: DataFrame = dfVisits_wd
      .groupBy("uid")
      .pivot("week_day_abb")
      .count.na.fill(0)

    logInfo("make day hours visits dataframe")
    val dfVisits_hr: DataFrame = dfVisits
      .withColumn("hour", concat(lit("web_hour_"),hour(col("tmstmp"))))
      .select("uid","hour")

    val matrix3: DataFrame = dfVisits_hr
      .groupBy("uid")
      .pivot("hour")
      .count.na.fill(0)

    logInfo("make fraction visits dataframe")
    val dfFraction: DataFrame = dfVisits
      .withColumn( "fraction",
        expr(
          "case when ((hour(tmstmp) >=9) AND (hour(tmstmp) < 18)) then 'web_fraction_work_hours'" +
            " when ((hour(tmstmp) >=18) AND (hour(tmstmp) < 24)) then 'web_fraction_evening_hours'" +
            " else 'night' end"))
      .select("uid","fraction")

    logInfo("count visits by uid")
    val dfFraction_cnt0: DataFrame = dfFraction.groupBy("uid","fraction").count

    val windowUID: WindowSpec = Window.partitionBy("uid")
    val dfFraction_cnt1: DataFrame = dfFraction_cnt0.groupBy("uid","fraction")
      .agg(sum("count").alias("count"))
      .withColumn("frctn", col("count") /  sum("count").over(windowUID))

    val matrix4 = dfFraction_cnt1.drop(col("count"))
      .groupBy("uid")
      .pivot("fraction")
      .sum("frctn").na.fill(0).drop("night")

    logInfo("join dataframes")
    val dfM1_M2: DataFrame = mtrx.join(matrix2,Seq("uid"))
    val dfM3_M4: DataFrame = matrix3.join(matrix4,Seq("uid"))
    val dfResult: DataFrame = dfM1_M2.join(dfM3_M4,Seq("uid"))

    logInfo("get users_items matrix")
    val dfUsersItems = spark.read.parquet("/user/ivan.shishkin/users-items/20200429")

    logInfo("join with result dataframe")
    val df2Save = dfResult.join(dfUsersItems,Seq("uid"),"left")

    df2Save.write.parquet("/user/ivan.shishkin/features")
  }
}
