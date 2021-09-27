import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, asc, count, desc, sum, sumDistinct}
import org.apache.log4j.{Level, Logger}

/*
Find out how many orders, how many products and how many sellers are in the data.
How many products have been sold at least once? Which is the product contained in more orders?
 */

object warmup01 extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Warm up 1")
    .getOrCreate()

  val path = "C:\\Users\\lucas.rubio\\OneDrive - Linx SA\\Área de Trabalho\\dataset\\"

  val products = spark.read.parquet(path + "products_parquet")
  val sales = spark.read.parquet(path + "sales_parquet")
  val sellers = spark.read.parquet(path + "sellers_parquet")

  println("Orders", sales.count())
  println("Products", products.count())
  println("Sellers", sellers.count())

  val uniqueProducts = sales.dropDuplicates("product_id").count()

  println("How many products have been sold at least once?", uniqueProducts)

  val mostFreqProducts = sales.groupBy("product_id")
    .agg(count("*").alias("count"))
    .orderBy(desc("count"))
    .limit(1)

  println("Which is the product contained in more orders?")
  mostFreqProducts.printSchema()
  mostFreqProducts.show()

}
