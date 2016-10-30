package com.github.ceason.mllibextras

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  *
  */
class LogLossEvaluator(val uid: String) extends Evaluator {



	def this() = this(Identifiable.randomUID("logLossEval"))

	val labelCol: Param[String] = new Param(this, "labelCol", "")
	val probabilityCol: Param[String] = new Param(this, "probabilityCol", "")


	def setLabelCol(name: String): this.type = {
		set(labelCol, name)
	}

	def setProbabilityCol(name: String): this.type = {
		set(probabilityCol, name)
	}


	override def evaluate(dataset: Dataset[_]): Double = {
		import dataset.sparkSession.implicits._

		val label = col($(labelCol))
		val probability = col($(probabilityCol))

		val logLosses = dataset
			.select(col($(labelCol)).cast(DoubleType), col($(probabilityCol)).cast(DoubleType))
			.as[(Double, Double)]
			.map(x ⇒ logLoss(
				label = x._1,
				probability = x._2))

		logLosses.rdd.sum / logLosses.count()
	}

	private def logLoss(label: Double, probability: Double): Double = {
		val epsilon = 1e-16

		if (label == 1)
			- Math.log(probability + epsilon)
		else
			- Math.log(1.0 - probability + epsilon)
	}




	override def isLargerBetter: Boolean = false

	def copy(extra: ParamMap): Evaluator = {
		val that = new LogLossEvaluator(uid)
		copyValues(that, extra)
	}
}
