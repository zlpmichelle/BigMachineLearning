package org.machine.learning.app

/**
 * Created by Michelle on 1/3/15.
 */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils


object decisionTreeExample {
  def main(args: Array[String]) {

    if (args.length != 5) {
      println("Parameters: (data: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vectors]; " +
        "datadir: String; numClasses: Int; impurity: String ('gini' or 'entropy' or 'variance'); maxDepth: Int; maxBins: Int")
      sys.exit(1)
    }

    // set up environment
    val conf = new SparkConf()
      .setAppName("decisionTree")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)


    // Load and parse the data file.
    // Cache the data since we will use it again to compute training error.
    //val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").cache()

    val dataDir = args(0)  // "/user/spark/USCensus1990raw.data-2.txt"

    // Train a DecisionTree model.
    val numClasses = args(1).toInt // 2
    val categoricalFeaturesInfo = Map[Int, Int]()  //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val impurity = args(2) // gini
    val maxDepth = args(3).toInt // 5
    val maxBins = args(4).toInt // 100


    // load and process training data
    val data = MLUtils.loadLibSVMFile(sc, dataDir).cache()

    val model = DecisionTree.trainClassifier(data, numClasses, categoricalFeaturesInfo, impurity,
      maxDepth, maxBins)

    // Evaluate model on training instances and compute training error
    val labelAndPreds = data.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / data.count
    println("Training Error = " + trainErr)
    println("Learned classification tree model:\n" + model)

  }
}
