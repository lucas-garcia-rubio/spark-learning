import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, countDistinct, desc}

/*
How many distinct products have been sold in each day?
 */

object warmup02 extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Warm up 2")
    .getOrCreate()

  val path = "C:\\Users\\lucas.rubio\\OneDrive - Linx SA\\Área de Trabalho\\dataset\\"

  val products = spark.read.parquet(path + "products_parquet")
  val sales = spark.read.parquet(path + "sales_parquet")
  val sellers = spark.read.parquet(path + "sellers_parquet")

  println("How many distinct products have been sold in each day?")
  sales.groupBy("date")
    .agg(countDistinct("product_id").alias("count_distinct"))
    .sort(desc("count_distinct"))
    .show()
}
