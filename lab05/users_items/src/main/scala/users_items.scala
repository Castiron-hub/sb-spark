import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}

object Utils {
    val checkPoint = "check"
    def delete(sc:SparkContext, path:String): Unit ={
        val fs = FileSystem.get(sc.hadoopConfiguration)
        val outPutPath = new Path(path)
        if (fs.exists(outPutPath)) {
            fs.delete(outPutPath, true)
            println(s"$path deleted.")
        }
    }

    def expr(myCols: Set[String], allCols: Set[String]) = {
        allCols.toList.map(x => x match {
            case x if myCols.contains(x) => col(x)
            case _ => lit(null).as(x)
        })
    }

    def getAndSaveDate(df:DataFrame)(implicit spark: SparkSession) = {
        import spark.implicits._
        val dat = df.select('date).groupBy().agg(max('date).alias("part"))
        dat.write.csv(checkPoint)
        val part = dat.collect().map(r=>r(0)).toList.head.toString
        part
    }

    def loadAndGetDate(implicit spark: SparkSession) = {
        val dat = spark.read.csv(checkPoint).select("*")
        dat.show()
        val part = dat.collect().map(r=>r(0)).toList.head.toString
        part
    }

    def readAndTransform(input_dir:String)(implicit spark: SparkSession) = {
        import spark.implicits._
        val viewDf = spark.read.json(input_dir + "/view")
        val buyDf = spark.read.json(input_dir + "/buy")
        val unionDF = buyDf.union(viewDf)
        val usersDF = unionDF.select("*")
          .withColumn("cat", concat(col("event_type"), lit("_"), regexp_replace(lower('item_id), "[- ]", "_")))
          .groupBy(col("uid")).pivot(col("cat")).count()
        val colNames = usersDF.schema.fields.map{x => x.name}
        val usersDF0 = usersDF.na.fill(0L, colNames)
        (usersDF0, getAndSaveDate(unionDF))
    }

    def modeUpdate(input_dir:String, output_dir:String, sc:SparkContext)(implicit spark: SparkSession) = {
        val part = "20200429"//loadAndGetDate
        delete(sc, checkPoint)
        println("Updating...")
        val userDFOld = spark.read.parquet(output_dir + "/" + part)
        val tuple = readAndTransform(input_dir)
        val userDFNew = tuple._1
        val cols1 = userDFOld.columns.toSet
        val cols2 = userDFNew.columns.toSet
        val total = cols1 ++ cols2
        userDFOld
          .select(expr(cols1, total):_*)
          .union(userDFNew.select(expr(cols2, total):_*))
          .write.parquet(output_dir + "/" + tuple._2)
        //tuple._1.union(userDFOld).write.parquet(output_dir + "/" + tuple._2)
    }

    def modeInsert(input_dir:String, output_dir:String, sc:SparkContext)(implicit spark: SparkSession) = {
        println("Try to delete checkpoint.")
        delete(sc, checkPoint)
        delete(sc, output_dir)
        println("Inserting...")
        val tuple = readAndTransform(input_dir)
        tuple._1.write.parquet(output_dir + "/" + tuple._2)
    }

}


object users_items extends App{
    override def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        val conf = new SparkConf().setAppName("ivan.shishkin:lab05")//.setMaster("local")
        val sc = new SparkContext(conf)
        implicit val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
        val mode = conf.get("spark.users_items.update").toInt
        implicit val output_dir = conf.get("spark.users_items.output_dir")
        implicit val input_dir = conf.get("spark.users_items.input_dir")
        println(s"mode: $mode")
        println(s"output_dir: $output_dir")
        println(s"input_dir: $input_dir")
        if (mode == 0)
            Utils.modeInsert(input_dir, output_dir, sc)
        else
            Utils.modeUpdate(input_dir, output_dir, sc)
        spark.stop
    }
}


