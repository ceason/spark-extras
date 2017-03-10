package org.apache.spark.ml.tuning

import com.github.fommil.netlib.F2jBLAS
import org.apache.spark.ml.Model
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Dataset

/**
  *
  */
class ParallelCrossValidator extends CrossValidator {

	private val f2jBLAS = new F2jBLAS


	override def fit(dataset: Dataset[_]): CrossValidatorModel = {
		val schema = dataset.schema
		transformSchema(schema, logging = true)
		val sparkSession = dataset.sparkSession
		val est = $(estimator)
		val eval = $(evaluator)
		val epm = $(estimatorParamMaps)
		val numModels = epm.length
		val metrics = new Array[Double](epm.length)
		val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
		splits.zipWithIndex.par.foreach { case ((training, validation), splitIndex) =>
			val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
			val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
			// multi-model training
			logDebug(s"Train split $splitIndex with multiple sets of parameters.")
			val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
			trainingDataset.unpersist()

			(0 until numModels).par.foreach { i ⇒
				val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)))
				logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
				metrics.synchronized {
					metrics(i) += metric
				}
			}
			validationDataset.unpersist()
		}
		f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
		logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
		val (bestMetric, bestIndex) =
			if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
			else metrics.zipWithIndex.minBy(_._1)
		logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
		logInfo(s"Best cross-validation metric: $bestMetric.")
		val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
		copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
	}
}
