hadoop fs -rm -r adult.test
hadoop fs -rm -r adult.out
if [ $1 -eq -1 ];
then
  hadoop fs -put adult.test adult.test
else
  head -$1 adult.test > temp1
  hadoop fs -put temp1 adult.test
  rm temp1
fi
export HADOOP_CMD=/usr/bin/hadoop
export HADOOP_STREAMING=/usr/lib/hadoop-mapreduce/hadoop-streaming-*.jar
Rscript bn-demo.R
hadoop fs -getmerge adult.out adult.out
