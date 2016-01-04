collection=$1

solrctl instancedir --create $collection /root/solr/$collection
solrctl collection --create $collection -s 3

nohup hadoop --config /etc/hadoop/conf.cloudera.yarn \
jar /opt/cloudera/parcels/CDH/lib/hbase-solr/tools/hbase-indexer-mr-1.5-cdh5.2.0-job.jar \
--conf /etc/hbase/conf.cloudera.hbase/hbase-site.xml \
-D 'mapred.child.java.opts=-Xmx500m' \
--hbase-indexer-file /root/solr/morphline-hbase-mapper.xml \
--zk-host $ZKHOME/solr --collection $collection \
--go-live --log4j /etc/hbase/conf.cloudera.hbase/log4j.properties --reducers 6

# for phoenix table
#!sql CREATE TABLE "test" (myPK VARCHAR PRIMARY KEY, "f"."q1" VARCHAR, "f"."q2" VARCHAR, "f"."q3" VARCHAR, "f"."q4" VARCHAR, "f"."q5" VARCHAR) SALT_BUCKETS = 6;