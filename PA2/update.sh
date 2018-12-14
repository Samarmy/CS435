#!/bin/bash
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Tfidf.java
jar cf Tfidf.jar *.class
#$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputTf1 
#$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputTf2 
#$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputIdf1 
#$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputIdf2
#$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputIdf3
#$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputSentence
#$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputJoin
$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputSentenceTfidf1
$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputSentenceTfidf2
$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputSentenceTfidf3
$HADOOP_HOME/bin/hadoop fs -rm -r /testFolder/OutputJoin2

$HADOOP_HOME/bin/hadoop jar Tfidf.jar Tfidf /testFolder/PA2Dataset /testFolder/OutputTf1 /testFolder/OutputTf2 /testFolder/OutputIdf1 /testFolder/OutputIdf2 /testFolder/OutputIdf3 /testFolder/OutputSentence /testFolder/OutputJoin /testFolder/OutputSentenceTfidf1 /testFolder/OutputSentenceTfidf2 /testFolder/OutputSentenceTfidf3 /testFolder/OutputJoin2

$HADOOP_HOME/bin/hadoop fs -cat /testFolder/OutputJoin2/*

