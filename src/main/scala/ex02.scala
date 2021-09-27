import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, broadcast, col}

/*
For each seller, what is the average % contribution of an order to the seller's daily quota?

# Example
If Seller_0 with `quota=250` has 3 orders:
Order 1: 10 products sold
Order 2: 8 products sold
Order 3: 7 products sold
The average % contribution of orders to the seller's quota would be:
Order 1: 10/105 = 0.04
Order 2: 8/105 = 0.032
Order 3: 7/105 = 0.028
Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333
 */

object ex02 extends App{

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

  // Author suggests broadcasting...

  sales.join(broadcast(sellers), "seller_id")
    .withColumn("individual_contribution", col("num_pieces_sold")/col("daily_target"))
    .groupBy("seller_id")
    .agg(avg("individual_contribution"))
    .show()

  /* ...but this is also ok.

  sales.join(sellers, "seller_id")
    .withColumn("individual_contribution", col("num_pieces_sold")/col("daily_target"))
    .groupBy("seller_id")
    .agg(avg("individual_contribution"))
    .show()

   */

}
