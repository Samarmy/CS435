#!/bin/bash                                                                    
#https://spark.apache.org/docs/latest/submitting-applications.html
$HADOOP_HOME/bin/hadoop fs -rm -r /pa3/Output3bomb
$SPARK_HOME/bin/spark-submit --executor-memory 7g --driver-memory 7g --class Spark --master yarn --deploy-mode cluster pa3-1.0-SNAPSHOT.jar /pa3/links/links-simple-sorted.txt /pa3/titles/titles-sorted.txt /pa3/Output3bomb bomb

$HADOOP_HOME/bin/hadoop fs -cat /pa3/Output3bomb/*
 
