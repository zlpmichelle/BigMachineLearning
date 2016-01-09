

package org.machine.learning.app

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Decision tree model for classification or regression.
 * This model stores the decision tree structure and parameters.
 * @param topNode root node
 * @param algo algorithm type -- classification or regression
 */
@Experimental
class DecisionTreeModelzlp(val topNode: Node, val algo: Algo) extends Serializable {

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param features array representing a single data point
   * @return Double prediction from the trained model
   */
  def predict(features: Vector): Double = {
    topNode.predict(features)

  }

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param features RDD representing data points to be predicted
   * @return RDD of predictions for each of the given data points
   */
  def predict(features: RDD[Vector]): RDD[Double] = {
    features.map(x => predict(x))
  }


  /**
   * Predict values for the given data set using the model trained.
   *
   * @param features JavaRDD representing data points to be predicted
   * @return JavaRDD of predictions for each of the given data points
   */
  def predict(features: JavaRDD[Vector]): JavaRDD[Double] = {
    predict(features.rdd)
  }

  /**
   * Get number of nodes in tree, including leaf nodes.
   */
  def numNodes: Int = {
    1 + topNode.numDescendants
  }

  /**
   * Get depth of tree.
   * E.g.: Depth 0 means 1 leaf node.  Depth 1 means 1 internal node and 2 leaf nodes.
   */
  def depth: Int = {
    topNode.subtreeDepth
  }

  /**
   * Print a summary of the model.
   */
  override def toString: String = algo match {
    case Classification =>
      s"DecisionTreeModel classifier of depth $depth with $numNodes nodes"
    case Regression =>
      s"DecisionTreeModel regressor of depth $depth with $numNodes nodes"
    case _ => throw new IllegalArgumentException(
      s"DecisionTreeModel given unknown algo parameter: $algo.")
  }

  /**
   * Print the full model to a string.
   */
  def toDebugString: String = {
    val header = toString + "\n"
    header + topNode.subtreeToString(2)
  }

}

