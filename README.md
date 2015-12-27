Simple Spark Application
============================

As usual, the 'Hello World!' of MR. 
A simple Spark application in Java that counts the occurrence of each word in a file and then counts the
occurrence of each character in the most popular* words. 

*over the given threshold.

To make the jar file:

    mvn clean package

To run from a gateway node in a CDH5.5 cluster:

This will run the application in a single local process.  
```./bin/spark-submit --class com.cloudera.wordcount.WordCount --master local --num-executors 1 --driver-memory 1g --executor-memory 1g --executor-cores 1 --queue default /home/cloudera/Downloads/sparkwordcount-0.0.1-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/input.txt 2```

If the cluster is running YARN, you can replace "--master local" with "--master yarn-client" or "--master yarn-cluster".
```./bin/spark-submit --class com.cloudera.wordcount.WordCount --master yarn-client --num-executors 1 --driver-memory 1g --executor-memory 1g --executor-cores 1 --queue default /home/cloudera/Downloads/sparkwordcount-0.0.1-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/input.txt 2```

```./bin/spark-submit --class com.cloudera.wordcount.WordCount --master yarn-cluster --num-executors 1 --driver-memory 1g --executor-memory 1g --executor-cores 1 --queue default /home/cloudera/Downloads/sparkwordcount-0.0.1-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/input.txt 2```

If the cluster is running a Spark standalone cluster manager, you can replace "--master local" with "--master spark://<master host>:<master port>".
```./bin/spark-submit --class com.cloudera.wordcount.WordCount --master spark://quickstart.cloudera:7077 --num-executors 1 --driver-memory 1g --executor-memory 1g --executor-cores 1 --queue default /home/cloudera/Downloads/sparkwordcount-0.0.1-SNAPSHOT.jar hdfs://quickstart.cloudera:8020/user/cloudera/input.txt 2```

Refer to Refer to http://spark.apache.org/docs/latest/running-on-yarn.html for more details on running a Spark application on yarn cluster.