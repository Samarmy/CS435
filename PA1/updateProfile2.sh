#!/bin/bash
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Profile2.java
jar cf Profile2.jar Profile2*.class
$HADOOP_HOME/bin/hadoop fs -rm -r testFolder/output2
$HADOOP_HOME/bin/hadoop fs -rm -r testFolder/outputProfile2

cd ..
$HADOOP_HOME/bin/hadoop jar example/Profile2.jar Profile2 testFolder/PA1DemoTestFile testFolder/output2 testFolder/outputProfile2
$HADOOP_HOME/bin/hadoop fs -cat testFolder/outputProfile2/*
