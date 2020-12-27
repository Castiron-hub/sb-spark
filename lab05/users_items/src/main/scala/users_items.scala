import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object users_items extends App {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL users items")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    var outDir: String = spark.conf.get("spark.users_items.output_dir")
    var inDir: String = spark.conf.get("spark.users_items.input_dir")

    println("out dir param is: " + outDir)
    println("inp dir param is: " + inDir)

    //задать дефолтный режим 1, а если задан параметр спарк то переопределить его
    val mode: String = try {
        spark.conf.get("spark.users_items.update", "1")
    } catch {
        case _: Throwable => println("mode default")
            "1"
    }

    def readJson(dir: String): DataFrame = {
        spark.read.json(dir)
          .na.drop(Seq("uid"))
          .withColumn("item_id", lower(col("item_id")))
          .withColumn("item_id", regexp_replace(col("item_id"), lit("[- ]"), lit("_")))
    }

    val sdfBuy = readJson(s"$inDir/buy/**/")
    val sdfView = readJson(s"$inDir/view/**/")

    def pivotDF(dataFrame: DataFrame): DataFrame = {
        dataFrame
          .groupBy("uid")
          .pivot("item_id")
          .agg(count("uid"))
    }

    val buyPiv = pivotDF(sdfBuy)
    val viewPiv = pivotDF(sdfView)

    val oldColumnsBuy = buyPiv.columns
    val oldColumnsView = viewPiv.columns

    def formatColumnNames(cols: Array[String], dfType: String): Array[String] = {
        cols
          .map(col =>  if (col.equals("uid")) col else dfType + col)
    }

    val newColumnsBuy = formatColumnNames(oldColumnsBuy,"buy_")
    val newColumnsView = formatColumnNames(oldColumnsView,"view_")

    def getColumns(oldCols: Array[String], newCols: Array[String]): Array[Column] = {
        oldCols.zip(newCols).map(f => col(f._1).as(f._2))
    }

    val columnsBuy = getColumns(oldColumnsBuy, newColumnsBuy)
    val columnsView = getColumns(oldColumnsView, newColumnsView)

    val buyRenamed = buyPiv.select(columnsBuy: _*).na.fill(0, newColumnsBuy)
    val viewRenamed = viewPiv.select(columnsView: _*).na.fill(0, newColumnsView)

    val fullJoin = buyRenamed.join(viewRenamed, Seq("uid"), "outer")

    val fullJoinCols = fullJoin.columns

    val dfAll = fullJoin.na.fill(0, fullJoinCols)

    val maxDate = spark.read.json(s"$inDir/**/**")
      .agg(max(col("date"))).first().getString(0)

    if (mode == "0") {
        dfAll.coalesce(1).write.mode(SaveMode.Overwrite).parquet(s"$outDir/$maxDate")
    }
    else if (mode == "1") {
        val previousDF = spark.read.parquet(s"$outDir/**")
        val previousUid = previousDF.select(col("uid")).rdd.map(r => r(0)).collect()
        val dfAppend = dfAll.where(!col("uid").isin(previousUid: _*))
        println("dfAppend schema is:")
        dfAppend.printSchema()
        dfAppend.coalesce(1).write.mode(SaveMode.Overwrite).parquet(s"$outDir/$maxDate")
    }

}
