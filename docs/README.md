# How to Run 4 Tasks

## Task 1-1

### 1. Compile the Scala Source Code
Compile the Scala source file using the Hadoop classpath:

```bash
scalac -classpath "$(hadoop classpath)" task_1-1.scala
```

### 2. Package compiled classes into a JAR file
```bash
jar cf task_1-1.jar *.class
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
hadoop jar task_1-1.jar Task1_1 /input/amazon_sale_report.csv /output/
```

**Note:**
The output directory must not already exist in HDFS. If it exists, delete it before running the job.

### 7. Retrieve output file
```bash 
hadoop fs -get /output/part-r-00000 Task_1-1.csv
```