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


object smartflyold {
  def main(args: Array[String]) {
    // set up environment
    val conf = new SparkConf()
      .setAppName("decisionTree")
      .set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)

    //val data = sc.textFile("hdfs://172.31.4.45:8020/user/hive/warehouse/ccp.db/flight_final/9a4bed3493f18d80-8fb52465161d9190_794782928_data.0.")
    val data = sc.textFile("hdfs://172.31.4.45:8020/user/hive/warehouse/ccp.db/flight_final_delay/454c9a57d781dd19-ce0691df3eec99bc_1112149617_data.0.")
    val parsedData = data.map(s => s.split(',').map(_.toDouble)).map(x => LabeledPoint(x(0), Vectors.dense(x.tail)))
    val splits = parsedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    // todo
    val moniV = data.map(s => s.split(',').map(_.toDouble)).sample(false, 0.00001).map(x => Vectors.dense(x.tail))
    val categoricalFeaturesInfo = Map(6 -> 17, 7 -> 280, 8 -> 280)

    val maxBins = 281
    val numClasses = 8
    val impurity = "variance" // gini, entropy, variance
    val maxDepth = 5

    println("============================== Start classification Decision Tree Model Training =============================== ")

    //val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    val model = DecisionTree.trainRegressor(training, categoricalFeaturesInfo, impurity, maxDepth, maxBins)


    println("============================== End Modeling =============================== ")
    println("Learned classification Decision Tree model:\n" + model)

    println("============================== Start Decision Tree Training Evaluation ============================")
    val labelAndPredsTrain = training.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    //val trainErr = labelAndPredsTrain.filter(r => (r._1 - r._2).abs >= 0.5).count.toDouble / training.count // Double = 0.4389481506379053
    //println("Training Error = " + trainErr)


    println("============================== Predict of Decision Tree Test ============================")
    println("Map the data points in test dataset to cluster indices as below: ")
    val labelAndPredsTest = test.map { point =>
      val prediction = model.predict(point.features)
      // val prevalue = calValue(prediction)
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
      // val prevalue = calValue(prediction)
      (point, prediction)
    }
    println("=======foreach predict====final")
    val data2 = labelAndPredsTestMoniv.collect
    for (i <- 0 until data2.size) {
      println(data2(i))
    }
    //labelAndPredsTest.foreach(println)

    /*println("=======saveAsFile")
    val resultPath = "hdfs://172.31.4.45:8020/tmp/dtresult.txt"
    labelAndPredsTest.saveAsTextFile(resultPath)

    println("------Result file : " + resultPath)*/

    val trainMSE = meanSquaredError(model, training)
    println("----Training MSE = " + trainMSE)

    val testMSE = meanSquaredError(model, test)
    println("----Test MSE = " + testMSE)


    val trainErr = labelAndPredsTest.filter(r => (r._1 - r._2).abs >= 0.5).count.toDouble / training.count  // Double = 0.4389481506379053
    println("Training Error = " + trainErr)

    //val accuracy = 1.0 * labelAndPredsTest.filter(x => x._1 == x._3).count() / test.count()
    //println("---Test accuracy = " + accuracy)

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



  def calValue(pred: Double): Double = {
   if(pred > 0.8){
     return 1.0
   }else{
     return 0.0
   }
  }

}

