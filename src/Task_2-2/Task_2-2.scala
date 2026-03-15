import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.funtions._

object Task2_2 {
    def main(args: Array[String]): Unit = {
        if (args.length != 2) {
            sys.exit(1) 
        }

        val inputPath = args(0)
        val outputPath = args(1)

        val spark = SparkSession.builder()
            .appName("Task_2-2")
            // .master("local[*]")
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._

        // Read data from csv file
        val rawDf = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(inputPath)

        // Count promotions in each (SKU, month, order)
        val step2Df = rawDf
            .withColumn("month", month(to_date(col("Date"), "MM-dd-yy")))
            .withColumn("count_promos", 
                when(col("promotion-ids").isNull, 0)
                .otherwise(size(split(col("promotion-ids"), ",")))
            )
            .select(col("SKU"), col("month"), col("Order ID"), col("Amount"), col("count_promos"))

        // Rank promotions of each order in a (SKU, month) partition
        val windowSpec = Window.partitionBy("SKU", "month").orderBy(col("count_promos").desc)
        val windowCount = Window.partitionBy("SKU", "month") 

        val step3Df = step2Df
            .withColumn("rank", row_number().over(windowSpec))
            .withColumn("total_orders", count("*").over(windowCount))

        // Filter out the orders which have the fifth-most promotions in each (SKU, month) partition
        val targetPromos = step3Df
            .filter(
                (col("total_orders") >= 5 && col("rank") === 5) || 
                (col("total_orders") < 5 && col("rank") === col("total_orders"))
            )
            .select(col("SKU").as("t_SKU"), col("month").as("t_month"), col("count_promos").as("target_count"))
        val finalOrders = step2Df
            .join(
                targetPromos, 
                col("SKU") === col("t_SKU") && col("month") === col("t_month") && col("count_promos") === col("target_count")
            )
        
        // Compute stddev of each (SKU, month)
        val finalResult = finalOrders
            .groupBy("SKU", "month")
            .agg(
                stddev_pop("Amount").as("std_raw"),
                count("Amount").as("num_orders") 
            )
            // If only 1 order --> stddev = 0
            .withColumn("std", when(col("num_orders") === 1, 0.0).otherwise(round(col("std_raw"), 4)))
            .select("SKU", "month", "std")

        // Create output file
        finalResult.coalesce(1)
            .write
            .mode("overwrite")
            .parquet(outputPath)

        spark.stop()
    }
}


