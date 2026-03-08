import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import config.Constants._
import java.io.File

object task2_1 {
    def main(args: Array[String]) : Unit = {
        val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("Task2_1")
        .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark.sparkContext

        import spark.implicits._

        try {
            val df = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(INPUT_PATH)

            val stateAvgs = df.filter(
                col("Fulfilment") === "Merchant" &&
                col("Courier Status") === "Shipped" && 
                col("Amount").isNotNull
            ).groupBy("ship-state").agg(avg("Amount").as("StateAvgAmount"))

            val filteredOrders = df.filter(
                col("ship-service-level") === "Standard" &&
                col("promotion-ids").isNotNull
            ).withColumn("ValidPromotionCount", {
                val promotionArray = split(col("promotion-ids"), ",")
                size(filter(promotionArray, p => !lower(p).contains("amazon")))
            }).filter(col("ValidPromotionCount") >= 3)

            val joinedDf = filteredOrders
            .join(stateAvgs, "ship-state")
            .filter(col("Amount") < col("StateAvgAmount"))

            val results = joinedDf
            .groupBy("ship-city")
            .agg(
                (count(when(col("Status") === "Cancelled", 1)) * 100.0 / count("*")).as("CancelledPercentage")
                // (count(when(col("Status").contains("Shipped"), 1)) * 100.0 / count("*")).as("CancelledPercentage")
            )
            .orderBy(desc("CancelledPercentage"))

            results.write
            .mode("overwrite")
            .parquet(s"$OUTPUT_DIR/Task_2-1.parquet")

            // val header = "City,CancelledPercentage"
            // val rows = results.map(row => {
            //     val city = Option(row.getAs[String]("ship-city")).getOrElse("Unknown")
            //     val pct = row.getAs[Any]("CancelledPercentage") match {
            //         case d: Double => f"$d%.2f"
            //         case _ => "0.00"
            //     }
            //     s"$city,$pct"
            // }).collect()

            // val directory = new java.io.File(OUTPUT_DIR)

            // if (!directory.exists()) {
            //     directory.mkdirs() 
            // }

            // val writer = new java.io.PrintWriter(new java.io.File(directory, "task2_1-results.csv"))
            // try {
            //     rows.foreach(writer.println)
            // } finally {
            //     writer.close()
            // } 
            
        } finally {
            spark.stop()
        }
    }
}