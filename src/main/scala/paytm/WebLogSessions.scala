package paytm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SQLContext, SaveMode, SparkSession}
import java.util.UUID

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType, TimestampType}

object WebLogSessions extends App {

  // Setting winutils.exe path for windows, this might differ on your system if using windows.
  System.setProperty("hadoop.home.dir", "C:\\winutil")

  val spark = SparkSession.builder().appName("WebLogSessions").master("local[*]").getOrCreate()
  import spark.implicits._

  // Reading in the given file
  val logsRDD = spark.read.textFile("data/2015_07_22_mktplace_shop_web_log_sample.log.gz").map(line => line.split(" (?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim))

  // Cleaning records that were not split properly (there were 12)
  val cleanedLogsRDD = logsRDD.withColumn("sizeValue", size(col("value"))).filter($"sizeValue" === "15")

  val webLogDF = cleanedLogsRDD.select("value").withColumn("timestamp", col("value").getItem(0).cast(TimestampType))
    .withColumn("elb", col("value").getItem(1).cast(StringType))
    .withColumn("client_port", col("value").getItem(2).cast(StringType))
    .withColumn("client_port_tmp", split($"client_port", "\\:"))
    .withColumn("client_ip", col("client_port_tmp").getItem(0).cast(StringType))
    .withColumn("client_port", col("client_port_tmp").getItem(1).cast(IntegerType))
    .withColumn("backend_port", col("value").getItem(3).cast(StringType))
    .withColumn("request_processing_time", col("value").getItem(4).cast(DoubleType))
    .withColumn("backend_processing_time", col("value").getItem(5).cast(DoubleType))
    .withColumn("response_processing_time", col("value").getItem(6).cast(DoubleType))
    .withColumn("elb_status_code", col("value").getItem(7).cast(IntegerType))
    .withColumn("backend_status_code", col("value").getItem(8).cast(IntegerType))
    .withColumn("received_bytes", col("value").getItem(9).cast(LongType))
    .withColumn("sent_bytes", col("value").getItem(10).cast(LongType))
    .withColumn("request", col("value").getItem(11).cast(StringType))
    .withColumn("request_tmp", split($"request", "\\ "))
    .withColumn("url", col("request_tmp").getItem(1))
    .withColumn("user_agent", col("value").getItem(12).cast(StringType))
    .withColumn("ssl_cipher", col("value").getItem(13).cast(StringType))
    .withColumn("ssl_protocol", col("value").getItem(14).cast(StringType))
    .drop("value").drop("client_port_tmp").drop("request_tmp")

  // 1. Sessionize web logs by IP, with a session window time of 15 minutes
  val prevTimeDF = webLogDF.select("client_ip", "url", "timestamp")
    .withColumn("previousTimestamp", lag("timestamp", 1)
    .over(Window.partitionBy("client_ip").orderBy("timestamp")))

  val newSessionDF = prevTimeDF
    .withColumn("is_new_session",
      when(unix_timestamp(col("timestamp")).minus(unix_timestamp(col("previousTimestamp"))) < lit(60 * 15),
        lit(0)).otherwise(lit(1)))

  val global_user_sessionIDDF = newSessionDF
    .withColumn("globalSessionID", sum("is_new_session").over(Window.orderBy("client_ip","timestamp")))
    .withColumn("userSessionID",sum("is_new_session").over(Window.partitionBy("client_ip").orderBy("timestamp")))

  global_user_sessionIDDF.limit(1000)
    .coalesce(1)
    .write
    .format("csv")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .save("data/output/weblogSessions_1")

  // 2. Average Session Time in seconds
  val sessionTimeDF = global_user_sessionIDDF
    .groupBy("client_ip", "userSessionID")
    .agg((unix_timestamp(max(col("timestamp"))) - unix_timestamp(min(col("timestamp"))))
      .as("sessionTime"))

  sessionTimeDF.agg(avg(col("sessionTime")))
    .write
    .format("csv")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .save("data/output/averageSessiontime_2")


  // 3. Unique URL visits per session
  global_user_sessionIDDF.groupBy("client_ip", "userSessionID")
    .agg(countDistinct("url").as("unique_url_count"))
    .limit(1000)
    .coalesce(1)
    .write
    .format("csv")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .save("data/output/distinctURL_3")

//   4. Top 10 users by IP that have the highest average session times.
  sessionTimeDF.groupBy("client_ip")
    .agg(avg(col("sessionTime")).as("avgUserSessionTime"))
    .orderBy(desc("avgUserSessionTime"))
    .limit(10)
    .coalesce(1)
    .write
    .format("csv")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .save("data/output/engagedUsers_4")
}