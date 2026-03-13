import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.io._
import java.net.URI

object Task1_1 {
    class StatisticsWritable extends Writable {
        var freq: Int = 0
        var sumAmount: Double = 0.0
        var sumAmountSquare: Double = 0.0
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

    class StateDateSizeKey extends WritableComparable[StateDateSizeKey] {
        var state: String = ""
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
    class CountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
        val one = new IntWritable(1)

        override def map(
            key: LongWritable,
            value: Text,
            context: Mapper[LongWritable, Text, Text, IntWritable]#Context
        ): Unit = {

        val line = value.toString

        if (line.contains("index,Order ID")) return

        val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

        if (
            fields(17).trim.nonEmpty &&
            fields(3).contains("Shipped") &&
            fields(13).trim.nonEmpty &&
            fields(15).trim.nonEmpty &&
            fields(13).toInt > 0
        ) {
            context.write(new Text(fields(17).trim), one)
        }
    }
  }
    class CountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

        override def reduce(
            key: Text,
            values: java.lang.Iterable[IntWritable],
            context: Reducer[Text, IntWritable, Text, IntWritable]#Context
        ): Unit = {
        var sum = 0
        val iterator = values.iterator()

        while (iterator.hasNext) {
            sum += iterator.next().get()
        }
        context.write(key, new IntWritable(sum))
    }
  }

    class MainMapper extends Mapper[LongWritable, Text, StateDateSizeKey, StatisticsWritable] {
        val formatter = DateTimeFormatter.ofPattern("MM-dd-yy")

        var windowMap = Map[String, Int]()

        override def setup(
            context: Mapper[
                LongWritable,
                Text,
                StateDateSizeKey,
                StatisticsWritable
            ]#Context
        ): Unit = {
            val cacheFiles = context.getCacheFiles
            if (cacheFiles != null && cacheFiles.length > 0) {
                val reader = new BufferedReader(new FileReader("state_counts"))

                var line = reader.readLine()

                while (line != null) {

                    val parts = line.split("\t")
                    val count = parts(1).toInt

                    windowMap += (parts(0) -> (if (count > 10000) 5 else 10))

                    line = reader.readLine()
                }
                reader.close()
            }
        }


        override def map(
            key: LongWritable,
            value: Text,
            context: Mapper[
                LongWritable,
                Text,
                StateDateSizeKey,
                StatisticsWritable
            ]#Context
        ): Unit = {

            val line = value.toString

            if (line.contains("index,Order ID")) return

            val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

            if (
                fields(17).trim.nonEmpty &&
                fields(3).contains("Shipped") &&
                fields(13).nonEmpty &&
                fields(15).nonEmpty &&
                fields(13).toInt > 0 &&
                fields(15).toDouble > 0.0
            ) {
                val state = fields(17).trim
                val date = LocalDate.parse(fields(2), formatter)
                val size = fields(10).trim

                val qty = fields(13).toInt
                val amount = fields(15).toDouble

                val windowSize = windowMap.getOrElse(state, 10)

                for (i <- 1 to windowSize) {
                    val windowDate = date.plusDays(i).format(formatter)
                    val outKey = new StateDateSizeKey(state, windowDate, size)
                    val outVal = new StatisticsWritable( qty, amount, amount * amount, 1)
                    context.write(outKey, outVal)
                }
            }
        }
    }
    class StateDateGroupComparator extends WritableComparator( classOf[StateDateSizeKey], true ) {
        override def compare(
            a: WritableComparable[_],
            b: WritableComparable[_]
        ): Int = {
            val k1 = a.asInstanceOf[StateDateSizeKey]
            val k2 = b.asInstanceOf[StateDateSizeKey]

            var res = k1.state.compareTo(k2.state)

            if (res == 0) res = k1.date.compareTo(k2.date)
            res
        }
    }

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
            context: Reducer[ StateDateSizeKey, StatisticsWritable, NullWritable, Text]#Context
        ): Unit = {
            val statisticsMap = scala.collection.mutable.Map[ String, (Int, Double, Double, Int)]()

            val iterator = values.iterator()

            while (iterator.hasNext) {
                val valueData = iterator.next()
                val currentSize = key.size

                val existing = statisticsMap.getOrElse( currentSize, (0, 0.0, 0.0, 0))

                statisticsMap(currentSize) = (
                    existing._1 + valueData.freq,
                    existing._2 + valueData.sumAmount,
                    existing._3 + valueData.sumAmountSquare,
                    existing._4 + valueData.count
                )
            }


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
        val tempPath = new Path("temp_counts")
        val job1 = Job.getInstance(conf, "Counting task")

        job1.setJarByClass(this.getClass)

        job1.setMapperClass(classOf[CountMapper])
        job1.setReducerClass(classOf[CountReducer])

        job1.setOutputKeyClass(classOf[Text])
        job1.setOutputValueClass(classOf[IntWritable])

        FileInputFormat.addInputPath(job1, new Path(args(0)))

        FileOutputFormat.setOutputPath(job1, tempPath)

        if (!job1.waitForCompletion(true)) System.exit(1)


        val job2 = Job.getInstance(conf, "Main Task")
        job2.setJarByClass(this.getClass)

        job2.addCacheFile(new URI(tempPath.toString + "/part-r-00000#state_counts"))

        job2.setMapperClass(classOf[MainMapper])
        job2.setReducerClass(classOf[MainReducer])

        job2.setGroupingComparatorClass(classOf[StateDateGroupComparator])

        job2.setMapOutputKeyClass(classOf[StateDateSizeKey])

        job2.setMapOutputValueClass(classOf[StatisticsWritable])

        job2.setOutputKeyClass(classOf[NullWritable])
        job2.setOutputValueClass(classOf[Text])

        FileInputFormat.addInputPath(job2, new Path(args(0)))

        FileOutputFormat.setOutputPath(job2, new Path(args(1)))

        System.exit(if (job2.waitForCompletion(true)) 0 else 1)
    }
}