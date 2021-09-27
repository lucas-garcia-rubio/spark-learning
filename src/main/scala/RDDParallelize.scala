import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object RDDParallelize {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder().
      master("local").
      appName("DataFrame Creation").
      getOrCreate()

    val structureData = Seq(
      Row("James","","Smith","36636","NewYork",3100),
      Row("Michael","Rose","","40288","California",4300),
      Row("Robert","","Williams","42114","Florida",1400),
      Row("Maria","Anne","Jones","39192","Florida",5500),
      Row("Jen","Mary","Brown","34561","NewYork",3000)
    )

    val structureSchema = new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("id",StringType)
      .add("location",StringType)
      .add("salary",IntegerType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), structureSchema
    )

    df.printSchema()
    df.show()

  }
}
