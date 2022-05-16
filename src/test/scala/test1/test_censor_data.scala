package test1
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, fixture}
import Censor_Data.Censor_Data.loadrdd
import Censor_Data.Censor_Data.loadDF
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

abstract class test_censor_data extends fixture.FunSuite with BeforeAndAfterAll {

  val schema_new = StructType(Array(StructField("Censor_id", StringType, true), StructField("Humidity", IntegerType, true)))
  @transient var spark : SparkSession = _
  override def beforeAll(): Unit =
    {
  spark = SparkSession.builder().master("local").getOrCreate()
    }

  override def afterAll(): Unit =
    {
      spark.stop()
    }
 test("File Loading")
  {
    val samplefile= loadrdd(spark,"C:\\Users\\Admin\\IdeaProjects\\June14_Practice2\\Data\\*.csv")
    assert(samplefile ==2 ,"Number of Files should be 2")
  }
  test("Record Processed")
  {
    val loaddataframe = loadDF(spark,schema_new,"C:\\Users\\Admin\\IdeaProjects\\June14_Practice2\\Data\\*.csv")
    val records = loaddataframe.count()
    assert(records ==7,"Number of Records should be 7")
  }
}
