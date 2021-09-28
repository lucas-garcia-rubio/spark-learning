import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/*
Who are the second most selling and the least selling persons (sellers) for each product?
Who are those for product with `product_id = 0`
 */

object ex03 extends App{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "3gb")
    .appName("Exercise 3")
    .getOrCreate()

  val path = "C:\\Users\\lucas.rubio\\OneDrive - Linx SA\\Área de Trabalho\\dataset\\"

  val products = spark.read.parquet(path + "products_parquet")
  val sales = spark.read.parquet(path + "sales_parquet")
  val sellers = spark.read.parquet(path + "sellers_parquet")

  val productSellerGrouped = sales.groupBy("product_id", "seller_id")
    .agg(sum("num_pieces_sold").as("num_pieces_sold"))

  val windowProdAsc = Window.partitionBy("product_id").orderBy(asc("num_pieces_sold"))
  val windowProdDesc = Window.partitionBy("product_id").orderBy(desc("num_pieces_sold"))

  productSellerGrouped.withColumn("rank", functions.dense_rank().over(windowProdAsc))
    .where("rank == 1")
  productSellerGrouped.withColumn("rank", functions.dense_rank().over(windowProdDesc))
    .where("rank == 2")
}
