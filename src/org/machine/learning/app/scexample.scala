package org.machine.learning.app

/**
 * Created by Michelle on 1/5/15.
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object scexample {
  def main(args: Array[String]) {
    // set up environment
    val conf = new SparkConf()
      .setAppName("decisionTree")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)


    val data = sc.textFile("hdfs://172.31.13.149:8020/user/hive/warehouse/nci.db/flight/5b48d95b80ad426e-4adbfbf0294e2b94_125866837_data.0.")
    val parsedData = data.map(s => s.split(',').map(_.toDouble))
    val training = parsedData.map(x => LabeledPoint(x(0), Vectors.dense(x.tail)))
    val categoricalFeaturesInfo = Map(8 -> 29, 9 -> 352, 10 -> 352)
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 353

    val model = DecisionTree.trainRegressor(training, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    model.predict(training.map(_.features))
    val labelsAndPredictions = training.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val trainMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}

    println("Training MSE = " + trainMSE)
    println("Learned classification tree model:\n" + model)

    //labelsAndPredictions.foreach(println);

  }

}

