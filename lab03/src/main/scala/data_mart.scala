import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory



object data_mart {

  def get_UID_DF (spark: SparkSession): DataFrame = {
    import com.datastax.spark.connector.cql.CassandraConnectorConf
    import org.apache.spark.sql.cassandra._
    //import com.datastax.spark.connector.rdd.ReadConf

    // set params for all clusters and keyspaces
    spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))
    spark.conf.set("spark.cassandra.connection.host", "10.0.0.5")
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "clients", "keyspace" -> "labdata" )).load()
  }

  def get_URL_visits_DF (spark: SparkSession, jsonName: String): DataFrame = {
    spark.read.json(jsonName) //"hdfs:///labs/laba03/weblogs.json")
      .select(col("uid"),explode(col("visits")).as("t"))
      .withColumn("host",lower(callUDF("parse_url", col("t.url"), lit("HOST"))))
      .withColumn("domain",regexp_replace(col("host"),"^([wW]+.)",""))
      .filter("host is not null")
      .filter("uid is not null")
      .groupBy("uid","domain")
      .count
    //  .orderBy("uid") //.show(truncate=false)
  }

  def get_URL_categories_DF (spark: SparkSession): DataFrame = {
    val df=spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.5:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "ivan_shishkin")
      .option("password", "wqARvbCF")
      .option("driver", "org.postgresql.Driver")
      .load()

    df.withColumn("cats",
      concat(lit("web_"),
        lower(regexp_replace(col("category"),"[.-]+","_"))))
  }

  def get_Shop_visites_DF(spark: SparkSession): DataFrame  = {
    val esOptions =
      Map(
        "es.nodes" -> "10.0.0.5:9200",
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true"
      )
    val df = spark.read.format("es")
      .options(esOptions)
      .load("visits")
      .filter("uid is not null")

    df.withColumn("cats",
      concat(lit("shop_"),
        lower(regexp_replace(col("category"),"[.-]+","_"))))
      .groupBy("uid","cats").count()
    //esDf.printSchema
    //esDf.show(1, 200, true)

    //dfCat.groupBy("uid","shop_cats").count
  }

  def main(args: Array[String]): Unit = {
    val JSON_NAME="hdfs:///labs/laba03/weblogs.json"

    val spark: SparkSession = SparkSession.builder
      .appName("Ivan.Shishkin: Lab03")
      .master("local")
      .config("spark.jars", "elasticsearch-spark-20_2.11-6.8.9.jar,postgresql-42.2.13.jar,spark-cassandra-connector_2.11-2.4.3.jar")
      .enableHiveSupport()
      .getOrCreate()


    //spark.sparkContext.setLogLevel("ERROR")

    // root
    // |-- uid: string (nullable = true)
    // |-- age: integer (nullable = true)
    // |-- gender: string (nullable = true)
    val UID_DF = get_UID_DF(spark)
    val dfUID_Age = UID_DF.withColumn("age_cat",
      expr("case when ((age >=18) AND (age <= 24)) then '18-24'" +
        "  when ((age >=25) AND (age <= 34)) then '25-34'" +
        "  when ((age >=35) AND (age <= 44)) then '35-44'" +
        "  when ((age >=45) AND (age <= 54)) then '45-54'" +
        "  when  (age >=55) then '>=55' else '' end")).drop("age")

    dfUID_Age.show()

    //root
    // |-- uid: string (nullable = true)
    // |-- domain: string (nullable = true)
    // |-- count: long (nullable = false)
    val dfVisits: DataFrame = get_URL_visits_DF(spark, JSON_NAME)
    dfVisits.show(truncate=false)



    //root
    //|-- uid: string (nullable = true)
    //|-- cats: string (nullable = true)
    //|-- count: long (nullable = false)
    val shopVisitsDF = get_Shop_visites_DF(spark)
    //shopVisitsDF.show()


    //root
    //|-- domain: string (nullable = true)
    //|-- category: string (nullable = true)
    //|-- cats: string

    val urlCategories: DataFrame = get_URL_categories_DF(spark)
    //  urlCategories.show()


    val dfVisitByCategory=urlCategories.join(dfVisits,Seq("domain"))
    //    dfVisitByCategory.show()
    //root
    // |-- uid: string (nullable = true)
    // |-- cats: string (nullable = true)
    // |-- cnt: long (nullable = true)

    val dfWebVisitsCnt=dfVisitByCategory
      .groupBy("uid","cats")
      .agg(sum("count").as("count"))


    val allVisitsDF = shopVisitsDF.union(dfWebVisitsCnt)
    //  val shopVisitsByCategoriesDF = shopVisitsDF
    //    .groupBy("uid","shop_cats").count.as("shopVisitCount")

    val result = allVisitsDF
      .groupBy("uid")
      .pivot("cats")
      .min("count").na.fill(0)

    val resultToPotgreDF = dfUID_Age.join(result,Seq("uid"),"left")
    resultToPotgreDF
      .write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.5:5432/ivan_shishkin")
      .option("dbtable", "clients")
      .option("user", "ivan_shishkin")
      .option("password", "wqARvbCF")
      .mode("append").save
    // shopVisitsByCategoriesDF.show()
  }
}
