package org.machine.learning.app

/**
 * Created by Michelle on 1/6/15.
 */
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, argmax => brzArgmax, sum => brzSum}
import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel

import org.apache.spark.{SparkException, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Model for Naive Bayes Classifiers.
 *
 * @param labels list of labels
 * @param pi log of class priors, whose dimension is C, number of labels
 * @param theta log of class conditional probabilities, whose dimension is C-by-D,
 *              where D is number of features
 */
class NaiveBayesModelzlp private[mllib] (
                                       val labels: Array[Double],
                                       val pi: Array[Double],
                                       val theta: Array[Array[Double]]) extends ClassificationModel with Serializable {

  val brzPi = new BDV[Double](pi)
  val brzTheta = new BDM[Double](theta.length, theta(0).length)

  {
    // Need to put an extra pair of braces to prevent Scala treating `i` as a member.
    var i = 0
    while (i < theta.length) {
      var j = 0
      while (j < theta(i).length) {
        brzTheta(i, j) = theta(i)(j)
        j += 1
      }
      i += 1
    }
  }

  override def predict(testData: RDD[Vector]): RDD[Double] = {
    val bcModel = testData.context.broadcast(this)
    testData.mapPartitions { iter =>
      val model = bcModel.value
      iter.map(model.predict)
    }
  }

  def predictProbability(testData: RDD[Vector]): RDD[Double] = {
    val bcModel = testData.context.broadcast(this)
    testData.mapPartitions { iter =>
      val model = bcModel.value
      iter.map(model.predictProbability)
    }
  }



  def predictProbability(testData: Vector): Double = {
    val v = brzPi + brzTheta * testData.toBreeze
    val index = brzArgmax(v)
    println("=======********* index ===================== " + index)
    println("=======********* v(index) ===================== " + v(index))
    //v(index)
    index
  }

  override def predict(testData: Vector): Double = {
    val v = brzPi + brzTheta * testData.toBreeze
    val index = brzArgmax(v)
    labels(index)
  }
}

/**
 * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
 *
 * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
 * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
 * document classification.  By making every vector a 0-1 vector, it can also be used as
 * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]). The input feature values must be nonnegative.
 */
class NaiveBayeszlp private (private var lambda: Double) extends Serializable with Logging {

  def this() = this(1.0)

  /** Set the smoothing parameter. Default: 1.0. */
  def setLambda(lambda: Double): NaiveBayeszlp = {
    this.lambda = lambda
    this
  }

  /**
   * Run the algorithm with the configured parameters on an input RDD of LabeledPoint entries.
   *
   * @param data RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   */
  def run(data: RDD[LabeledPoint]) = {
    val requireNonnegativeValues: Vector => Unit = (v: Vector) => {
      val values = v match {
        case sv: SparseVector =>
          sv.values
        case dv: DenseVector =>
          dv.values
      }
      if (!values.forall(_ >= 0.0)) {
        throw new SparkException(s"Naive Bayes requires nonnegative feature values but found $v.")
      }
    }

    // Aggregates term frequencies per label.
    // TODO: Calling combineByKey and collect creates two stages, we can implement something
    // TODO: similar to reduceByKeyLocally to save one stage.
    val aggregated = data.map(p => (p.label, p.features)).combineByKey[(Long, BDV[Double])](
      createCombiner = (v: Vector) => {
        requireNonnegativeValues(v)
        (1L, v.toBreeze.toDenseVector)
      },
      mergeValue = (c: (Long, BDV[Double]), v: Vector) => {
        requireNonnegativeValues(v)
        (c._1 + 1L, c._2 += v.toBreeze)
      },
      mergeCombiners = (c1: (Long, BDV[Double]), c2: (Long, BDV[Double])) =>
        (c1._1 + c2._1, c1._2 += c2._2)
    ).collect()
    val numLabels = aggregated.length
    var numDocuments = 0L
    aggregated.foreach { case (_, (n, _)) =>
      numDocuments += n
    }
    val numFeatures = aggregated.head match { case (_, (_, v)) => v.size }
    val labels = new Array[Double](numLabels)
    val pi = new Array[Double](numLabels)
    val theta = Array.fill(numLabels)(new Array[Double](numFeatures))
    val piLogDenom = math.log(numDocuments + numLabels * lambda)
    var i = 0
    aggregated.foreach { case (label, (n, sumTermFreqs)) =>
      labels(i) = label
      val thetaLogDenom = math.log(brzSum(sumTermFreqs) + numFeatures * lambda)
      pi(i) = math.log(n + lambda) - piLogDenom
      var j = 0
      while (j < numFeatures) {
        theta(i)(j) = math.log(sumTermFreqs(j) + lambda) - thetaLogDenom
        j += 1
      }
      i += 1
    }

    new NaiveBayesModelzlp(labels, pi, theta)
  }
}

/**
 * Top-level methods for calling naive Bayes.
 */
object NaiveBayeszlp {
  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
   * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
   * document classification.  By making every vector a 0-1 vector, it can also be used as
   * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]).
   *
   * This version of the method uses a default smoothing parameter of 1.0.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   */
  def train(input: RDD[LabeledPoint]): NaiveBayesModelzlp = {
    new NaiveBayeszlp().run(input)
  }

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
   * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
   * document classification.  By making every vector a 0-1 vector, it can also be used as
   * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]).
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter
   */
  def train(input: RDD[LabeledPoint], lambda: Double): NaiveBayesModelzlp = {
    new NaiveBayeszlp(lambda).run(input)
  }
}
