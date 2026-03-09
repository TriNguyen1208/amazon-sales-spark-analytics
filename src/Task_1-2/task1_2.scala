import org.apache.spark.sql.SparkSession
import config.Constants._

object task1_2 {
    def main(args: Array[String]) : Unit = {
        System.setProperty("hadoop.home.dir", "C:\\winutils")
        
        // Initialize Spark Session for local execution using all available CPU cores
        val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("Task1_2")
        .getOrCreate()

        import spark.implicits._

        spark.sparkContext.setLogLevel("ERROR")
        val sc = spark.sparkContext

        try {
            // Load raw text from CSV and remove the header row to prevent processing titles as data
            val raw_data = sc.textFile(INPUT_PATH)
            val header = raw_data.first()
            val data = raw_data.filter(row => row != header && row.trim.nonEmpty)

            // Extracts specific columns and filters for clothing sizes XXL and larger (e.g., 3XL, 4XL)
            val filtered_data = data.flatMap(line => {
                val cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

                val state = cols(17).trim
                if (state.nonEmpty && state != "") {
                  val month = cols(2).split("-")(0)
                  val style_id = cols(7).trim
                  val sku = cols(8).trim
                  val size = cols(10).trim

                  // Logic check: "XXL" contains "XL" but is not equal to "XL"
                  if (size.contains("XL") && size != "XL") {
                    Some(((month, state, style_id), sku))
                  } else None
                } else None
            })

          // Removes duplicate SKUs within the same style, then counts unique styles per month/state
          val varieties = filtered_data
          .distinct()
          .map {case ((month, state, style_id), sku) => ((month, state), 1)}
          .reduceByKey(_ + _)

          // Groups all variety counts by (Month, State) to calculate the median of those counts
          val results = varieties
          .groupByKey()
          .mapValues(vList => {
            val sorted = vList.toList.sorted
            val n = sorted.size

            if (n % 2 == 0) (sorted(n/2 - 1) + sorted(n/2)) / 2.0
            else sorted(n/2).toDouble
          })

          // Re-orders the key to (State, Month) to allow alphabetical sorting by State
          val grouped_results = results.map{case((month, state), median) => ((state, month), median)}.sortByKey()

          // Manual CSV construction: Combine a custom header RDD with the data RDD
          val header_output = sc.parallelize(Seq("State,Month,Median"))
          val rows = grouped_results.map { case ((state, month), median) => 
              s"$state,$month,$median"
          }

          val final_output = header_output.union(rows)
          
          // Coalesce(1) merges all partitions into a single part-00000 file
          final_output.coalesce(1).saveAsTextFile(OUTPUT_DIR + "/task1-2")

        } finally {
          spark.stop()
        }
    }
}
