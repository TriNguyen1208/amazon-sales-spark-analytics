import org.apache.spark.sql.SparkSession
import config.Constants._

object task1_2 {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Task1_2")
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    try {
      val rawData = sc.textFile(INPUT_PATH)
      val header = rawData.first()
      val data = rawData.filter(row => row != header && row.trim.nonEmpty)

      val filteredData = data.flatMap(line => {
        val cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

        val state = cols(17).trim
        if (state.nonEmpty && state != "") {
          val dateStr = cols(2)
          val dateParts = dateStr.split("-")
          val month = dateParts(0)
          
          val styleId = cols(7).trim
          val sku = cols(8).trim
          val size = cols(10).trim
          if (size.contains("XL") && size != "XL") {
            Some(((month, state, styleId), sku))
          } else None
        } else None
      })

      val varieties = filteredData
      .distinct()
      .map {case ((month, state, styleId), sku) => ((month, state), 1)}
      .reduceByKey(_ + _)

      val results = varieties
      .groupByKey()
      .mapValues(vList => {
        val sorted = vList.toList.sorted
        val n = sorted.size

        if (n % 2 == 0) (sorted(n/2 - 1) + sorted(n/2)) / 2.0
        else sorted(n/2).toDouble
      })

      val groupedResults = results.map{case((month, state), median) => ((state, month), median)}.sortByKey()

      val output = groupedResults.map {case((month, state), median) => s"$month,$state,$median"}
      val headerOutput = sc.parallelize(Seq("State,Month,Median"))
      val finalOutput = headerOutput.union(output)

      val directory = new java.io.File(OUTPUT_DIR)

      if (!directory.exists()) {
          directory.mkdirs() 
      }

      val csvResults = finalOutput.collect()
      val writer = new java.io.PrintWriter(new java.io.File(directory, "Task_1-2.csv"))
      try {
        csvResults.foreach(writer.println)
      } finally {
        writer.close()
      }

    } finally {
      spark.stop()
    }
  }
}