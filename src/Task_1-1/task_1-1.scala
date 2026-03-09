import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.annotation.meta.field

object task1_1 {
    case class ProductBought (state: String, date: LocalDate, size: String, qty: Int, amount: Double)
    case class Statistics (freq: Int, sum_amount: Double, sum_amount_square: Double, count: Int)

    val sizeOrder = Map(
        "XS" -> 1,
        "S" -> 2,
        "M" -> 3,
        "L" -> 4,
        "XL" -> 5,
        "XXL" -> 6,
        "3XL" -> 7,
        "4XL" -> 8,
        "5XL" -> 9,
        "6XL" -> 10,
        "Free" -> 11
    )   

    val formatter = DateTimeFormatter.ofPattern("MM-dd-yy")
    
    def variance(statistic: Statistics) : Double = {
        val mean = statistic.sum_amount / statistic.count
        ((statistic.sum_amount_square / statistic.count) - (mean * mean))
    }
    def compare_better(a: (String, Statistics), b: (String, Statistics)) : (String, Statistics) = {
        val (size_1, statistics_1) = a
        val (size_2, statistics_2) = b

        if(statistics_1.freq == statistics_2.freq){
            val variance1 = variance(statistics_1)
            val variance2 = variance(statistics_2)

            if(variance1 == variance2){
                if (sizeOrder(size_1) < sizeOrder(size_2)) a else b
            }else if (variance1 < variance2) a
            else b
        }else if (statistics_1.freq < statistics_2.freq) b
        else a
    }

    def main(args: Array[String]) : Unit = {
        // if (args.length < 2) {
        //     System.err.println("Usage: task_1-1 <input_path> <output_path>")
        //     System.exit(1)
        // }
        val INPUT_PATH = "/Users/ductri0981/Documents/Scala/amazon-sales-spark-analytics/src/Amazon Sale Report.csv"
        val OUTPUT_PATH = "Task1-1.csv"

        val spark = SparkSession
                    .builder()
                    .master("local[*]")
                    .appName("Task1_1")
                    .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        
        val sc = spark.sparkContext
        val rawData = sc.textFile(INPUT_PATH)
        val header = rawData.first()

        //Step 1: Filter bought in rawData. Get fields (state, date, size, qty, amount)
        val bought = rawData
                    .filter(_ != header)
                    .map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
                    .filter(fields => fields(3).contains("Shipped") && fields(13).trim.nonEmpty && fields(15).trim.nonEmpty && fields(13).toInt > 0)
                    .map(field => ProductBought(
                            field(17).trim,
                            LocalDate.parse(field(2),formatter),
                            field(10).trim,
                            field(13).toInt,
                            field(15).toDouble
                        )
                    )
                    .cache()
        
        //Step 2: Count bought per state
        val state_count = bought
                        .map(product => (product.state,1))
                        .reduceByKey(_+_)

        //Step 3: Compute window size in each state
        val window_map = state_count
                        .mapValues(count => if(count > 10000) 5 else 10)
                        .collectAsMap()

        val window_slide = sc.broadcast(window_map)

        //Step 4: Create busket for sliding window. Each product bought contributed to next (d + 1, d + window_size)
        val busket = bought.flatMap { product => 
            val window_size = window_slide.value.getOrElse(product.state, 10) //Get window_size or default is 10
            (1 to window_size).map { i =>
                val window_date = product.date.plusDays(i)
                val key = (product.state, window_date, product.size) //This is key to contributed next day
                val value = Statistics(product.qty, product.amount, product.amount * product.amount, 1) //This is value to contributed next day
                (key, value)
            }            
        }
        //Busket hien tai gom co
        // [info] ((MAHARASHTRA,2022-04-11,L),Statistics(1,788.0,1))
        // [info] ((MAHARASHTRA,2022-04-12,L),Statistics(1,788.0,1))
        // [info] ((MAHARASHTRA,2022-04-13,L),Statistics(1,788.0,1))
        // [info] ((MAHARASHTRA,2022-04-14,L),Statistics(1,788.0,1))
        // [info] ((MAHARASHTRA,2022-04-15,L),Statistics(1,788.0,1))

        //Step 5: Reduced value (freq, sum_amount, sum_amount_square, count) with the same key (state, date, size)
        val reduced = busket.reduceByKey{ (product_1, product_2) => 
            Statistics(
                product_1.freq + product_2.freq,
                product_1.sum_amount + product_2.sum_amount,
                product_1.sum_amount_square + product_2.sum_amount_square,
                product_1.count + product_2.count
            )
        }

        //Step 6: Group the size in the state, date
        val grouped = reduced.map { record => 
            val key = record._1
            val value = record._2

            val state = key._1
            val date = key._2
            val size = key._3

            ((state, date), (size, value))
        }.groupByKey()

        //((Odisha,2022-05-26),CompactBuffer((M,Statistics(1,399.0,1)), (S,Statistics(1,399.0,1))))
        //Step 7: Compare in Statistic (freq, amount, count), if freq < freq return larger one, else return variance smaller, else return S < L
        val result = grouped.mapValues{ sizes =>
            sizes.reduce((a,b) => compare_better(a,b))
        }
        // Step 8: output
        val output = result.map {case((state, date), (size, _)) => s"$state,${date.format(formatter)},$size"}
        val headerOutput = sc.parallelize(Seq("state,window_date,size"))
        val finalOutput = headerOutput.union(output)

        finalOutput.saveAsTextFile(OUTPUT_PATH)
        spark.stop()
    }
}