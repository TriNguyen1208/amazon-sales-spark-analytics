import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import config.Constants._
import java.io.File

object task2_1 {
    def main(args: Array[String]) : Unit = {
        System.setProperty("hadoop.home.dir", "C:\\winutils")
        
        // Initialize Spark Session for local execution using all available CPU cores
        val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("Task2_1")
        .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark.sparkContext

        import spark.implicits._

        try {
            // Load CSV with automatic type detection (Schema Inference)
            val df = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(INPUT_PATH)

            // Filters for specific merchant-fulfilled, shipped orders to establish a baseline
            val state_avgs = df.filter(
                col("Fulfilment") === "Merchant" &&
                col("Courier Status") === "Shipped" && 
                col("Amount").isNotNull
            ).groupBy("ship-state").agg(avg("Amount").as("StateAvgAmount"))

            // Uses the 'withColumn' transformation to process nested string data in 'promotion-ids'
            val filtered_orders = df.filter(
                col("ship-service-level") === "Standard" &&
                col("promotion-ids").isNotNull

            ).withColumn("ValidPromotionCount", {
                // Split string into array, filter out 'amazon' promotions, and count remaining elements
                val promotion_array = split(col("promotion-ids"), ",")
                size(filter(promotion_array, p => !lower(p).contains("amazon")))

            }).filter(col("ValidPromotionCount") >= 3)

            // Join local orders with state averages to find "below-average" spenders
            val joined_df = filtered_orders
            .join(state_avgs, "ship-state")
            .filter(col("Amount") < col("StateAvgAmount"))

            // Group by city to find the ratio of cancelled orders to total orders
            val results = joined_df
            .groupBy("ship-city")
            .agg(
                (count(when(col("Status") === "Cancelled", 1)) * 100.0 / count("*")).as("CancelledPercentage")
            )
            .orderBy(desc("CancelledPercentage"))

            results.write
            .mode("overwrite")
            .parquet(OUTPUT_DIR + "/task2-1")
            
        } finally {
            spark.stop()
        }
    }
}
