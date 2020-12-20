import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.streaming.Trigger



//
object agg {

    //
    def main(args: Array[String]): Unit = {
        (new agg( SparkSession.builder.getOrCreate )).run()
    }

}

//
class agg(spark: SparkSession) {
    import spark.implicits._

    //
    case class Visit ( event_type : String
                     , category   : String
                     , item_id    : String
                     , item_price : Long
                     , uid        : String
                     , timestamp  : Long )

    //
    def run(): Unit = {
        println("########## Deleting checkpoints ##########")
        val checkpointLocation = "lab04b/checkpoint"
        FileSystem.get(new Configuration).delete(new Path(checkpointLocation), true)

        //
        while ( ! spark.read.format("kafka")
                       .option("kafka.bootstrap.servers", "spark-master-1:6667")
                       .option("subscribe", "ivan_shishkin")
                       .load
                       .isEmpty ) {
            println("########## Awaiting empty input ##########")
            Thread.sleep(10000)
        }

        //
        println("########## Awaiting input ##########")
        val s = ScalaReflection.schemaFor[Visit]
                               .dataType
                               .asInstanceOf[StructType]
        spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", "spark-master-1:6667")
             .option("subscribe", "ivan_shishkin")
             .load
             .select( from_json('value.cast("string"), s) alias "json" )
             .selectExpr("json.*")
             .withColumn("ts", window(('timestamp/1000).cast("timestamp"), "1 hour") )
             .select( 'ts("start").cast("long") alias "start_ts"
                    , 'ts("end")  .cast("long") alias "end_ts"
                    , when('event_type === "buy", 1) alias "f_buy"
                    , 'item_price
                    , 'uid )
             .groupBy('start_ts, 'end_ts)
             .agg( sum(when('f_buy > 0, 'item_price)) alias "revenue"
                 , count('uid)                        alias "visitors"
                 , count('f_buy)                      alias "purchases" )
             .withColumn("aov", when('purchases > 0, 'revenue/'purchases))
             .toJSON
             .withColumn("key", get_json_object('value, "$.start_ts"))
             .writeStream
             /*.format("console")
             .option("numRows", "1000")
             .option("truncate", "false")*/
             .format("kafka")
             .option("kafka.bootstrap.servers", "10.0.0.5:6667")
             .option("topic", "ivan_shishkin_lab04b_out")
             .option("checkpointLocation", checkpointLocation)
             .trigger(Trigger.ProcessingTime("5 seconds"))
             .outputMode("update")
             .start
             .awaitTermination
    }

}
