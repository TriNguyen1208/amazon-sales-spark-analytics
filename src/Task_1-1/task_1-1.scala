import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.{Path, FileSystem}


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.io._
import java.net.URI

object Task1_1 {
    //Custom Writable class used to store statistic variable that can be transfer between Hadoop node
    class StatisticsWritable extends Writable {
        //Number of qty of each tuple (state, window_size, size)
        var freq: Int = 0
        
        //Sum of amount
        var sumAmount: Double = 0.0
        
        //Sum of square amount that used for calculate variance
        var sumAmountSquare: Double = 0.0

        //Counting each type that used for calculate variance.
        var count: Int = 0

        def this(freq: Int, sumAmount: Double, sumAmountSquare: Double, count: Int) = {
            this()
            this.freq = freq
            this.sumAmount = sumAmount
            this.sumAmountSquare = sumAmountSquare
            this.count = count
        }

        override def write(out: DataOutput): Unit = {
            out.writeInt(freq)
            out.writeDouble(sumAmount)
            out.writeDouble(sumAmountSquare)
            out.writeInt(count)
        }

        override def readFields(in: DataInput): Unit = {
            freq = in.readInt()
            sumAmount = in.readDouble()
            sumAmountSquare = in.readDouble()
            count = in.readInt()
        }

        override def toString: String = s"$freq|$sumAmount|$sumAmountSquare|$count"
    }

    //Composite key used to group and sort records
    class StateDateSizeKey extends WritableComparable[StateDateSizeKey] {
        var state: String = ""
        //Window date
        var date: String = ""
        var size: String = ""

        def this(state: String, date: String, size: String) = {
            this()
            this.state = state
            this.date = date
            this.size = size
        }

        override def write(out: DataOutput): Unit = {
            out.writeUTF(state)
            out.writeUTF(date)
            out.writeUTF(size)
        }

        override def readFields(in: DataInput): Unit = {
            state = in.readUTF()
            date = in.readUTF()
            size = in.readUTF()
        }

        override def compareTo(o: StateDateSizeKey): Int = {
            var res = this.state.compareTo(o.state)
            if (res == 0) res = this.date.compareTo(o.date)
            if (res == 0) res = this.size.compareTo(o.size)
            res
        }
    }
    //Mapper class used to count the number of bought orders by state
    class CountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
        val one = new IntWritable(1)

        override def map(
            key: LongWritable,
            value: Text,
            context: Mapper[LongWritable, Text, Text, IntWritable]#Context
        ): Unit = {

            val line = value.toString

            //Skip the header line
            if (line.contains("index,Order ID")) return
            
            //Split CSV line.
            val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

            val state = fields(17)
            val qty = fields(13)
            val amount = fields(15)
            val status = fields(3)

            // Filter record:
            // State must not empty
            // Order has “shipped” in its status
            // quantity and amount must not be empty
            // quantity and amount must be greater than 0
            if (
                state.trim.nonEmpty &&
                status.contains("Shipped") &&
                qty.trim.nonEmpty &&
                amount.trim.nonEmpty &&
                qty.toInt > 0 &&
                amount.toDouble > 0.0
            ) {
                //Emit (state, 1) so reducer can count total bought orders per state
                context.write(new Text(state.trim.toUpperCase), one)
            }
        }
    }
    //Reducer class that aggregates the counts of bought orders for each state
    class CountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
        override def reduce(
            key: Text,
            values: java.lang.Iterable[IntWritable],
            context: Reducer[Text, IntWritable, Text, IntWritable]#Context
        ): Unit = {
            var sum = 0
            // Iterator to go through all values emitted by the mapper for the same key
            val iterator = values.iterator()

            while (iterator.hasNext) {
                // Sum all occurrences (each mapper emits 1)
                sum += iterator.next().get()
            }
            context.write(key, new IntWritable(sum))
        }
    }

    //It emits a composite key (state, date, size) along with statistical values.
    class MainMapper extends Mapper[LongWritable, Text, StateDateSizeKey, StatisticsWritable] {
        // Formatter used to parse dates in the format MM-dd-yy
        val formatter = new SimpleDateFormat("MM-dd-yy")

        // Map storing window size for each state
        var windowMap = Map[String, Int]()

        override def setup(
            context: Mapper[LongWritable, Text, StateDateSizeKey, StatisticsWritable]#Context
        ): Unit = {
            // Retrieve files added to Hadoop Distributed Cache
            val cacheFiles = context.getCacheFiles
            //If cache file exists, read the counting_bought_order file
            if (cacheFiles != null && cacheFiles.length > 0) {
                val reader = new BufferedReader(new FileReader("counting_bought_order"))

                var line = reader.readLine()

                while (line != null) {
                    // Each line format: state \t count
                    val parts = line.split("\t")
                    val count = parts(1).toInt
                    // Determine window size based on number of records
                    windowMap += (parts(0) -> (if (count > 10000) 5 else 10))
                    line = reader.readLine()
                }
                reader.close()
            }
        }


        override def map(
            key: LongWritable,
            value: Text,
            context: Mapper[LongWritable, Text, StateDateSizeKey, StatisticsWritable]#Context
        ): Unit = {

            val line = value.toString

            if (line.contains("index,Order ID")) return

            val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
            val state = fields(17)
            val date = formatter.parse(fields(2))
            val size = fields(10).trim

            val qty = fields(13)
            val amount = fields(15)
            val status = fields(3)
            if (
                state.trim.nonEmpty &&
                status.contains("Shipped") &&
                qty.trim.nonEmpty &&
                amount.trim.nonEmpty &&
                qty.toInt > 0 &&
                amount.toDouble > 0.0
            ) {
                val windowSize = windowMap.getOrElse(state.trim.toUpperCase, 10)
                val calendar = Calendar.getInstance()
                // Generate sliding windows for the next N days
                // Each record contributes to multiple future window dates
                for (i <- 1 to windowSize) {
                    // Calculate the date for the current window
                    calendar.setTime(date)
                    calendar.add(Calendar.DAY_OF_MONTH, i)

                    val windowDate = formatter.format(calendar.getTime)
                    // Create composite key (state, windowDate, size)
                    val outKey = new StateDateSizeKey(state.trim.toUpperCase, windowDate, size)
                    val outVal = new StatisticsWritable( qty.toInt, amount.toDouble, amount.toDouble * amount.toDouble, 1)
                    // Emit key-value pair for reducer aggregation
                    context.write(outKey, outVal)
                }
            }
        }
    }
    //Custom grouping comparator used by Hadoop during the shuffle phase.
    //It groups keys by state and date only, ignoring the size field.
    class StateDateGroupComparator extends WritableComparator( classOf[StateDateSizeKey], true ) {
        override def compare(
            a: WritableComparable[_],
            b: WritableComparable[_]
        ): Int = {
            val k1 = a.asInstanceOf[StateDateSizeKey]
            val k2 = b.asInstanceOf[StateDateSizeKey]

            var res = k1.state.compareTo(k2.state)

            if (res == 0) res = k1.date.compareTo(k2.date)
            // Size is intentionally ignored for grouping
            res
        }
    }

    //Reducer that determines the best-selling size for each (state, date) window
    class MainReducer extends Reducer[ StateDateSizeKey, StatisticsWritable, NullWritable, Text] {
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
        // Compute variance of order amount
        // variance = E(x²) - (E(x))²
        def variance(
            sumAmount: Double,
            sumAmountSquare: Double,
            count: Int
        ): Double = {
            val mean = sumAmount / count
            (sumAmountSquare / count) - (mean * mean)
        }

        override def reduce(
            key: StateDateSizeKey,
            values: java.lang.Iterable[StatisticsWritable],
            context: Reducer[StateDateSizeKey, StatisticsWritable, NullWritable, Text]#Context
        ): Unit = {
            // size -> (total frequency, sumAmount, sumAmountSquare, record count)
            val statisticsMap = scala.collection.mutable.Map[ String, (Int, Double, Double, Int)]()

            val iterator = values.iterator()
            // Aggregate statistics from mapper outputs
            while (iterator.hasNext) {
                val valueData = iterator.next()
                val currentSize = key.size
                // Retrieve existing statistics if present
                val existing = statisticsMap.getOrElse( currentSize, (0, 0.0, 0.0, 0))

                statisticsMap(currentSize) = (
                    existing._1 + valueData.freq,
                    existing._2 + valueData.sumAmount,
                    existing._3 + valueData.sumAmountSquare,
                    existing._4 + valueData.count
                )
            }

            // Determine the best size based on:
            // 1. Highest frequency
            // 2. If tie, choose smaller variance
            // 3. If tie again, choose based on predefined size order
            val bestSize = statisticsMap.reduce { (a, b) =>
                val (size_a, (freq_a, sum_a, sum_sq_a, count_a)) = a
                val (size_b, (freq_b, sum_b, sum_sq_b, count_b)) = b

                if (freq_a == freq_b) {
                    val var_a = variance(sum_a, sum_sq_a, count_a)
                    val var_b = variance(sum_b, sum_sq_b, count_b)

                    if (var_a == var_b) {
                        if (sizeOrder(size_a) < sizeOrder(size_b)) a
                        else b
                    } else if (var_a < var_b) a
                    else b
                } else if (freq_a < freq_b) b
                else a
            }._1
            // Output: state, date, bestSize
            context.write( NullWritable.get(), new Text(s"${key.state},${key.date},$bestSize"))
        }
    }


    def main(args: Array[String]): Unit = {
        if (args.length != 2) {
            System.err.println(
                "Task_1-1 need to have both <input path> and <output path>"
            )
            System.exit(1)
        }
        val conf = new Configuration()
        // Temporary path used to store output of Job 1
        val tempPath = new Path("temp_counting_bought_order")
        val job1 = Job.getInstance(conf, "Counting bought order")

        job1.setJarByClass(this.getClass)
        // Set Mapper and Reducer for counting
        job1.setMapperClass(classOf[CountMapper])
        job1.setReducerClass(classOf[CountReducer])

        // Output key/value types
        job1.setOutputKeyClass(classOf[Text])
        job1.setOutputValueClass(classOf[IntWritable])

        FileInputFormat.addInputPath(job1, new Path(args(0)))
        FileOutputFormat.setOutputPath(job1, tempPath)

        if (!job1.waitForCompletion(true)) System.exit(1)


        val job2 = Job.getInstance(conf, "Main Job")
        job2.setJarByClass(this.getClass)

        // Add Job1 result file to Hadoop Distributed Cache
        // It will be available to mappers as "counting_bought_order"
        job2.addCacheFile(new URI(tempPath.toString + "/part-r-00000#counting_bought_order"))

        // Set Mapper and Reducer for main computation
        job2.setMapperClass(classOf[MainMapper])
        job2.setReducerClass(classOf[MainReducer])

        // Custom grouping comparator to group by (state, date)
        job2.setGroupingComparatorClass(classOf[StateDateGroupComparator])
        // Map output types
        job2.setMapOutputKeyClass(classOf[StateDateSizeKey])
        job2.setMapOutputValueClass(classOf[StatisticsWritable])

        // Final output types
        job2.setOutputKeyClass(classOf[NullWritable])
        job2.setOutputValueClass(classOf[Text])

        FileInputFormat.addInputPath(job2, new Path(args(0)))
        FileOutputFormat.setOutputPath(job2, new Path(args(1)))
        val job2_success = job2.waitForCompletion(true)
        if (job2_success){
            val fs = FileSystem.get(conf)
            if (fs.exists(tempPath)) {
                fs.delete(tempPath, true)
            }
        }
        System.exit(if (job2_success) 0 else 1)
    }
}