package org.machine.learning.app

/**
 * Created by Michelle on 1/6/15.
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD



import scala.language.reflectiveCalls

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo, Strategy}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


object backp1 {
  def main(args: Array[String]) {
    // set up environment
    val conf = new SparkConf()
      .setAppName("decisionTree")
      .set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)

    // history flights data
    val data = sc.textFile("hdfs://172.31.4.45:8020/user/hive/warehouse/p1.db/flight_final_delay/c948d63b0c8f20ad-2fdbcc7af8c7d084_860328325_data.0.")
    val parsedData = data.map(s => s.split(',').map(_.toDouble)).map(x => LabeledPoint(x(0), Vectors.dense(x.tail)))
    val splits = parsedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // scheduled flights data
    val scheduledData = sc.textFile("hdfs://172.31.4.45:8020/user/hive/warehouse/p1.db/scheduled_final/f64cfeebeddfe667-3bcfd65fba217ec0_930871996_data.0.").map(x => x.split(",").toList).map(x => (x.head, x.tail))

    val moniV = data.map(s => s.split(',').map(_.toDouble)).sample(false, 0.00001).map(x => Vectors.dense(x.tail))
    val categoricalFeaturesInfo = Map(6 -> 19, 7 -> 285, 8 -> 285)

    val maxBins = 286
    val numClasses = 8
    val impurity = "variance" // gini, entropy, variance
    val maxDepth = 5

    println("============================== Start classification Decision Tree Model Training =============================== ")
    val model = DecisionTree.trainRegressor(training, categoricalFeaturesInfo, impurity, maxDepth, maxBins)


    println("============================== End Modeling =============================== ")
    println("Learned classification Decision Tree model:\n" + model)

    println("============================== Start Decision Tree Training Evaluation ============================")
    val labelAndPredsTrain = training.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    println("============================== Predict of Decision Tree Test ============================")
    println("Map the data points in test dataset to cluster indices as below: ")
    val labelAndPredsTest = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    println("=======for435335")
    val data1 = labelAndPredsTest.sample(false, 0.00001).collect
    for (i <- 0 until data1.size) {
      println(data1(i))
    }

    //todo
    println("=======foreach predict")
    val labelAndPredsTestMoniv = moniV.map { point =>
      val prediction = model.predict(point)
      (point, prediction)
    }
    println("=======foreach predict====final")
    val data2 = labelAndPredsTestMoniv.collect
    for (i <- 0 until data2.size) {
      println(data2(i))
    }

    // scheduled flight data
    println("=======scheduled")
    val scheduledDataPreds = scheduledData.map { point => {
      val uniqueFlightID = point._1
      val a = point._2.map(_.toDouble)
      val b = Vectors.dense(a.toArray)
      val prediction = model.predict(b)
      (0,uniqueFlightID, prediction,0)
    }
    }

    println("=======foreach scheduled====scheduled")
    scheduledDataPreds.take(10).foreach(println)


    val resultPath = "hdfs://172.31.4.45:8020/tmp/ccp/p1/result"
    scheduledDataPreds.saveAsTextFile(resultPath)
    println("=======saveAsFile: " + resultPath)

    val trainMSE = meanSquaredError(model, training)
    println("----Training MSE = " + trainMSE)

    val testMSE = meanSquaredError(model, test)
    println("----Test MSE = " + testMSE)
    println("============================== End of Decision Tree Test ============================")
    sc.stop()

  }

  /**
   * Calculates the mean squared error for regression.
   */
  def meanSquaredError(model: {def predict(features: Vector): Double},
                       data: RDD[LabeledPoint]): Double = {
    data.map { y =>
      val err = model.predict(y.features) - y.label
      err * err
    }.mean()
  }

}

