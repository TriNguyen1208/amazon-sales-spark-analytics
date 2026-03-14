import java.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.collection.JavaConverters._

import org.apache.log4j.{Level, Logger, BasicConfigurator}

object task1_2 {
  // BasicConfigurator.configure()
  // Logger.getRootLogger.setLevel(Level.INFO)
  // // This will show you exactly why the job failed
  // Logger.getLogger("org.apache.hadoop.mapred.LocalJobRunner").setLevel(Level.DEBUG)

  // Count unique SKUs per style
  class SKUCountMapper extends Mapper[LongWritable, Text, Text, Text] {
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      try {
        val line = value.toString
        if (line.startsWith("index") || line.startsWith("\"index\"")) return
        
        val cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
        val month = cols(2).split("-")(0)
        val style_id = cols(7).trim
        val sku = cols(8).trim
        val size = cols(10).trim
        val state = cols(17).trim
        
        if (state.nonEmpty && ((size.contains("XL") && size != "XL") || size == "Free")) {
          context.write(new Text(s"$month\t$state\t$style_id"), new Text(sku))
        }
      } catch {
        case e: Exception => 
          println(s"ERROR at line: ${value.toString.take(50)}... -> ${e.getMessage}")
      }
    }
  }

  class SKUCountReducer extends Reducer[Text, Text, Text, IntWritable] {
    override def reduce(key: Text, value: java.lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
      try {
        val distinct_skus = value.asScala.map(_.toString).toSet

        val parts = key.toString.split("\t")
        context.write(new Text(s"${parts(0)}\t${parts(1)}"), new IntWritable(distinct_skus.size))
      } catch {
        case e: Exception => 
          println(s"[SKUCountReducer]: ${e.getMessage}")
      }
    }
  }

  class MedianMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
      val parts = value.toString.split("\t")
      if (parts.length == 3) {
        context.write(new Text(s"${parts(0)},${parts(1)}"), new IntWritable(parts(2).toInt))
      }
    }
  }

  class MedianReducer extends Reducer[Text, IntWritable, Text, DoubleWritable] {
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, DoubleWritable]#Context): Unit = {
      val counts = values.asScala.map(_.get()).toList.sorted
      
      val n = counts.size
      val median = if (n % 2 == 0) {
        (counts(n / 2 - 1) + counts(n / 2)) / 2.0
      } else {
        counts(n / 2).toDouble
      }
      context.write(key, new DoubleWritable(median))
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <input_path> <output_path>")
      System.exit(-1)
    }

    // System.setProperty("user.name", "KhoiPhan")
    // System.setProperty("HADOOP_USER_NAME", "KhoiPhan")
    // System.setProperty("hadoop.home.dir", "D:\\Download\\hadoop\\hadoop-3.3.6")

    val conf = new Configuration()

    val customTemp = "./hadoop_temp_internal"
    conf.set("hadoop.tmp.dir", customTemp)
    conf.set("mapreduce.cluster.local.dir", s"$customTemp/local")
    conf.set("mapreduce.cluster.temp.dir", s"$customTemp/temp")

    conf.set("mapreduce.framework.name", "local")

    val tempPathString = "./hadoop_temp_job1"
    val tempDir = new Path(tempPathString)
    val finalOutputDir = new Path(args(1))

    val fs = tempDir.getFileSystem(conf)
    if (fs.exists(tempDir)) {
      fs.delete(tempDir, true) 
      println(s"Deleted old temp directory: $tempPathString")
    }
    if (fs.exists(finalOutputDir)) {
      fs.delete(finalOutputDir, true)
      println(s"Deleted old final output directory: ${args(1)}")
    }

    // Job 1 Configuration
    val job1 = Job.getInstance(conf, "Variety Count Scala")
    job1.setJarByClass(this.getClass)
    job1.setMapperClass(classOf[SKUCountMapper])
    job1.setReducerClass(classOf[SKUCountReducer])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job1, new Path(args(0)))
    FileOutputFormat.setOutputPath(job1, tempDir)

    val job1_success = job1.waitForCompletion(true)

    if (job1_success) {
      // Job 2 Configuration
      val job2 = Job.getInstance(conf, "Median Calculation Scala")
      job2.getConfiguration.set("mapreduce.output.textoutputformat.separator", ",")
      job2.setJarByClass(this.getClass)
      job2.setMapperClass(classOf[MedianMapper])
      job2.setReducerClass(classOf[MedianReducer])
      
      job2.setMapOutputKeyClass(classOf[Text])
      job2.setMapOutputValueClass(classOf[IntWritable])
      
      job2.setOutputKeyClass(classOf[Text])
      job2.setOutputValueClass(classOf[DoubleWritable])
      
      FileInputFormat.addInputPath(job2, tempDir)
      FileOutputFormat.setOutputPath(job2, finalOutputDir)
      
      val job2_success = job2.waitForCompletion(true)

      if (job2_success) {
        println("All jobs finished successfully. Starting cleanup the temp dirs")
        
        val fs = FileSystem.get(conf)
        
        if (fs.exists(tempDir)) {
            fs.delete(tempDir, true)
        }
        
        // Delete the internal Hadoop scratch directory
        val internalTempPath = new Path(customTemp)
        if (fs.exists(internalTempPath)) {
            fs.delete(internalTempPath, true)
        }
      }

      System.exit(if (job2_success) 0 else 1)
    }
  }
}