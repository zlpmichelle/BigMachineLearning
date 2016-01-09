spark-submit --master spark://ip-172-31-4-45:7077 --class smartfly --total-executor-cores 4  /opt/dt.jar
sudo -u hdfs hadoop fs -rm -r /tmp/dtresult.txt


spark-submit --master spark://ip-172-31-4-45:7077 --class org.apache.spark.mllib.classification.bayes --total-executor-cores 4  /opt/dt.jar

sudo -u hdfs hadoop fs -rm -r /tmp/bayesresult.txt


spark-shell --driver-memory 2g --executor-memory 8g