cd $HADOOP_CONF_DIR
for i in `cat workers`
do
ssh $i "rm -rf /s/$i/a/tmp/hadoop-$USER"
done

