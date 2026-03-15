# How to Run 4 Tasks

## Task 1-1 and Task 1-2

The following instruction shows how to run the Task 1-1 code (Task 1-2 is the same).

### 1. Compile the Scala Source Code
Compile the Scala source file using the Hadoop classpath:

```bash
scalac -classpath "$(hadoop classpath)" task1_1.scala
```

### 2. Package compiled classes into a JAR file
```bash
jar cf task1_1.jar *.class
```

### 3. Add Scala libraries to Hadoop
Hadoop is built on Java and requires Scala libraries to execute Scala-based applications.
```bash
sudo cp -L /usr/share/java/scala-library.jar $HADOOP_HOME/share/hadoop/common/lib/
```

Restart Hadoop after adding the library:

```bash
stop-dfs.sh
start-dfs.sh
```

### 4. Rename given csv file
```bash
mv "Amazon Sale Report.csv" amazon_sale_report.csv
```

### 5. Upload input file to HDFS
```bash
hadoop fs -put amazon_sale_report.csv /input/
```

### 6. Execute the task_1-1 job
```bash
hadoop jar task1_1.jar task1_1 /input/amazon_sale_report.csv /output/
```

**Note:**
The output directory must not already exist in HDFS. If it exists, delete it before running the job.

### 7. Retrieve output file
```bash 
hadoop fs -get /output/part-r-00000 Task_1-1.csv
```

## Task 2-1 and Task 2-2

Standing at the folder containing the code file and follow the steps below to run Task 2-1 code (Task 2-2 would be the same).

### 1. Build .sbt file
- Create a build.sbt file.
```bash
nano build.sbt
```
- Write the file as below:
```bash
name := "Task21" 
version := "1.0"
scalaVersion := "2.13.12"
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "4.1.1", 
    "org.apache.spark" %% "spark-sql"  % "4.1.1"
)
```
**Note:** You can change the versions above to which you're using.

### 2. Rename and upload input file to HDFS
**Note:** You can skip this step if you have done it when running Task 1-1/Task 1-2.
```bash
mv "Amazon Sale Report.csv" amazon_sale_report.csv
hadoop fs -mkdir -p /input/
hadoop fs -put amazon_sale_report.csv /input/
```

### 3. Compile the code
```bash
sbt package
```

**Note:** After running the command line above, go to *target/scala-2.\*/* to check if the .jar file has been created. In this case, a file named *Task21_2.13-1.0.jar* should be created.

### 4. Run the code
```bash
spark-submit \
  --class task2_1 \
  --master yarn \
  --deploy-mode cluster \
  ./target/scala-2.13/task21_2.13-1.0.jar \
  hdfs:///input/amazon_sale_report.csv \
  hdfs:///output/Task_2-1.parquet
```

### 5. Retrieve the output file
```bash
hdfs dfs -get /output/Task_2-1.parquet/part-*.parquet Task_2-1.parquet
```

