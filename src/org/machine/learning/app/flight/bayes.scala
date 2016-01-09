/**
 * Created by Michelle on 1/6/15.
 */

package org.machine.learning.app.flight

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, argmax => brzArgmax, sum => brzSum}
import breeze.stats.distributions.Multinomial
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.{NaiveBayeszlp, NaiveBayes}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}


object bayes {
  def main(args: Array[String]) {
    // set up environment
    val conf = new SparkConf()
      .setAppName("decisionTree")
      .set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)

    //val data = sc.textFile("hdfs://172.31.4.45:8020/user/hive/warehouse/ccp.db/flight_final/9a4bed3493f18d80-8fb52465161d9190_794782928_data.0.").cache();
    val data = sc.textFile("hdfs://172.31.4.45:8020/user/hive/warehouse/ccp.db/flight_final_delay/454c9a57d781dd19-ce0691df3eec99bc_1112149617_data.0.").cache();
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(if (parts(0).toInt == 1) 1 else 0, Vectors.dense(parts(1).toDouble, parts(2).toDouble, parts(3).toDouble, parts(4).toDouble,
        parts(5).toDouble, parts(6).toDouble, parts(7).toDouble, parts(8).toDouble, parts(9).toDouble, parts(10).toDouble))
    }

    val splits = parsedData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // todo training
    //MultinomialNaiveBayes
    //BinarizedNaiveBayes
    val model = NaiveBayeszlp.train(training, lambda = 1.0)
    println("============================== End Modeling =============================== ")
    println("Learned classification Bayes model:\n" + model)


    // todo training
    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    // obtain probability
    val probabilities = model.predictProbability(test.map(_.features))


    val labelAndPredsTest = test.map { point =>
      val prediction = model.predict(point.features)
      val probability = model.predictProbability(point.features)
      (point.label, prediction, probability)
    }


    println("=======for435335")
    val data1 = labelAndPredsTest.sample(false,0.00001).collect
    for(i <- 0 until data1.size){
      println(data1(i))
    }

    println("-------------- result maxtrix: ")
    val onev = Vectors.dense(2014.0,10.0,19.0,7.0,19.0,20.0,4.0,0.0,127.0,100.0)
    val v = model.brzPi + model.brzTheta * (onev.toBreeze)
    for(i <- 0 until v.size){
      print(v(i))
    }

    println("----brzArgmax(v): " + brzArgmax(v));


    println("=======for breeze")
    val vp = model.pi
    val vt = model.theta
    val vpb = model.brzPi
    val vtb = model.brzTheta
    println("-------------pi : " + vp)
    println("-------------theta : " + vt)
    for(i <- 0 until vt.size){
      for(j <- 0 until vt(i).length ){
        print(vt(i)(j) + " ")
      }
      println()
    }
    println("-------------braPi : " + vpb)
    println("-------------braTheta : " + vtb)



    //println("=======foreach predict")
    //predictionAndLabel.foreach(println) // no output


    /*println("=======saveAsFile")
    val resultPath = "hdfs://172.31.4.45:8020/tmp/bayesresult.txt"
    predictionAndLabel.saveAsTextFile(resultPath)
    println("------Result file : " + resultPath)*/
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("Test accuracy = " + accuracy)
  }

}