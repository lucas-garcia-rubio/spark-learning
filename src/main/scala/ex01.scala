import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.functions.{avg, col}

/*
What is the average revenue of the orders?
 */

object ex01 extends App{

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

  val sales_join_products = sales.join(products, "product_id")

  sales_join_products.withColumn("revenue", col("price")*col("num_pieces_sold"))
    .agg(avg("revenue"))
    .show()

}
