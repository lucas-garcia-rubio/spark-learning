import org.apache.spark.sql.SparkSession

/*
Who are the second most selling and the least selling persons (sellers) for each product?
Who are those for product with `product_id = 0`
 */

object ex03 extends App{

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

}
