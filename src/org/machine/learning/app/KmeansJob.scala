package org.machine.learning.app

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors

object kmeans {
  def main(args: Array[String]) {
   
    if (args.length != 5) {
      println("Parameters: (data: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vectors]; " +
        "k: Int; maxIterations: Int; runs: Int (default:1); initializationMode: String ('random' or 'k-means||'(default))")
      sys.exit(1)
    }

    // set up environment
    val conf = new SparkConf()
      .setAppName("Kmeans")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    // load parameters
    val dataDir = args(0)  // "/user/spark/USCensus1990raw.data-2.txt"
    val numClusters = args(1).toInt           // 5
    val numIterations = args(2).toInt         // 20  
    val numRuns = args(3).toInt               // default is 1
    val initializationMode = args(4)    // "random" or "k-means||" (default)
    
    // load and process training data
    val data = sc.textFile(dataDir)   
    val newData = data.map(s => s.replaceAll("P", "111").split('\t').filter(!_.isEmpty).map(_.toDouble))
    val parsedData = newData.map(s => Vectors.dense(s))
    
    val splits = parsedData.randomSplit(Array(0.9, 0.1), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1) 

    println("============================== Start Clustering =============================== ")
    val clusters = KMeans.train(training, numClusters, numIterations, numRuns, initializationMode)
    println("============================== End Modeling =============================== ")
    println("Created  " + clusters.k + "  clusters")
    println("Cluster Centers:")
    clusters.clusterCenters.foreach(println)

// val clusterCenters: Array[Vector]  
// def predict(points: RDD[Vector]): RDD[Int]  Maps given points to their cluster indices.   Maps given points to their cluster indices.
// def predict(point: Vector): Int   Returns the cluster index that a given point belongs

    println("============================== Start Clustering Evaluation ============================")
    val WSSSE = clusters.computeCost(training)
    println("Within Set Sum of Squared Errors on Training Data = " + WSSSE)
    println("Map the data points in test dataset to cluster indices as below: ")

    println("============================== Predict of Clustering Test ============================")
    clusters.predict(test).foreach(println)
    
    println("============================== End of Clustering Test ============================")
    sc.stop()
  }
}
