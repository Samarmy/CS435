#!/bin/bash                                                                    
#https://spark.apache.org/docs/latest/submitting-applications.html
$HADOOP_HOME/bin/hadoop fs -rm -r /term_project/OutputTest

$SPARK_HOME/bin/spark-submit --executor-memory 7g --driver-memory 7g --class Kcluster --master yarn --deploy-mode cluster term_project-1.0-SNAPSHOT.jar 9 10 10 /term_project/2DClustering.txt /term_project/OutputTest AB AB
$HADOOP_HOME/bin/hadoop fs -cat /term_project/OutputTest/*
 
