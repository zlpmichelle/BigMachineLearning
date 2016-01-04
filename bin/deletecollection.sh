collection=$1
solrctl collection --delete $collection
solrctl collection --delete $collection
solrctl instancedir --delete $collection
sudo -u hdfs hadoop fs -rm -r /solr/$collection
rm -rf nohup.out
