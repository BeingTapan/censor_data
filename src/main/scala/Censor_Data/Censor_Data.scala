package Censor_Data

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{avg, col, max, min}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Censor_Data extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Union_Concepts").getOrCreate()
    if(args.length == 0)
      {
        Logger.getLogger("path not provided")
        System.exit(1)
      }

    val schema_new = StructType(Array(StructField("Censor_id", StringType, true), StructField("Humidity", IntegerType, true)))
     spark.sparkContext.setLogLevel("ERROR")


    printf("Number of Files Processed :" +loadrdd(spark,args(0)))

   val loadCensordata = loadDF(spark,schema_new,args(0))
    loadCensordata.show(10,false)
    println("Number of  processed Measurements :",+loadCensordata.count())
    val df3 = loadCensordata.filter(col("Humidity").isNull)
    println("Number of failed Measurements :",+df3.count())
    val df2 = loadCensordata.groupBy ("Censor_id").agg(min("Humidity").alias("min"),max("Humidity").alias("max"),avg("Humidity").alias("avg")).orderBy(col("avg").desc)
    df2.show(false)

  }

  def loadrdd(spark : SparkSession,pathfile : String): Long= {
    val sc = spark.sparkContext
    val rdd = sc.wholeTextFiles(pathfile)
    val rdd1 = rdd.map(f => f._1)
    rdd1.count()
  }
  def loadDF(spark : SparkSession,schema_new : StructType,pathfile : String): DataFrame = {
       spark.read.option("header","true").schema(schema_new).csv(pathfile)
  }
}
