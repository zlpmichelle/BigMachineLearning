package org.machine.learning.app.flight

/**
 * Created by Michelle on 1/5/15.
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors


object scexample2 {
  def main(args: Array[String]) {
    // set up environment
    val conf = new SparkConf()
      .setAppName("decisionTree")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://172.31.13.149:8020/user/hive/warehouse/nci.db/flight/5b48d95b80ad426e-4adbfbf0294e2b94_125866837_data.0.")
    val parsedData = data.map(s => s.split(',').map(_.toDouble)).map(x => LabeledPoint(x(0),Vectors.dense(x.tail)))
    val splits = parsedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    val categoricalFeaturesInfo = Map(8->29,9->352,10->352)

    val maxBins = 353
    val numClasses = 8
    val impurity = "gini"
    val maxDepth = 5

    val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    val labelAndPreds = training.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val trainErr = labelAndPreds.filter(r => (r._1 - r._2).abs >= 0.5).count.toDouble / training.count  // Double = 0.4389481506379053

    println("Training Error = " + trainErr)
    println("Learned classification tree model:\n" + model)
    //labelAndPreds.foreach(println);

  }

}
