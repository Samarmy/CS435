#!/bin/bash
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Profile3.java
jar cf Profile3.jar Profile3*.class
$HADOOP_HOME/bin/hadoop fs -rm -r testFolder/output3
$HADOOP_HOME/bin/hadoop fs -rm -r testFolder/outputProfile3

cd ..
$HADOOP_HOME/bin/hadoop jar example/Profile3.jar Profile3 testFolder/PA1DemoTestFile testFolder/output3 testFolder/outputProfile3
$HADOOP_HOME/bin/hadoop fs -cat testFolder/output3/*
echo
$HADOOP_HOME/bin/hadoop fs -cat testFolder/outputProfile3/*
