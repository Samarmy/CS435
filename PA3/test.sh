#!/bin/bash                                                                    
#https://spark.apache.org/docs/latest/submitting-applications.html
$HADOOP_HOME/bin/hadoop fs -rm -r /pa3/Output2
$SPARK_HOME/bin/spark-submit --class Spark --master yarn --deploy-mode cluster spark-1.0-SNAPSHOT.jar /pa3/Input.txt /pa3/Input2.txt /pa3/Output2 false

$HADOOP_HOME/bin/hadoop fs -cat /pa3/Output2/*
 
