#!/bin/bash
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Profile1.java
jar cf Profile1.jar Profile1*.class
$HADOOP_HOME/bin/hadoop fs -rm -r testFolder/outputProfile1
cd ..
$HADOOP_HOME/bin/hadoop jar example/Profile1.jar Profile1 testFolder/PA1DemoTestFile testFolder/outputProfile1
$HADOOP_HOME/bin/hadoop fs -cat testFolder/outputProfile1/*
