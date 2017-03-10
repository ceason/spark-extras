package com.github.ceason.mllibextras.kaggle

import java.text.SimpleDateFormat
import java.util.Date

import com.github.ceason.mllibextras.ParallelPipeline
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParallelCrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  *
  *
  * @param labeledData      labeled dataset that we train with
  * @param unlabeledData    what we submit to be scored
  * @param labelCol         aka the column that's not in the unlabeled dataset ;)
  * @param evaluator        metric for model fitness (eg rmse, logloss, etc)
//  * @param estimator        AKA the model to train (eg random forest, svm, etc)
  * @param paramGridBuilder hyperparameters we'll gridsearch over
  * @param transformers     sequence of data transformations to apply before training model
  */
case class KaggleRecipe(
	labeledData: DataFrame,
	unlabeledData: DataFrame,
	labelCol: String,
	predictionCol: String,
	evaluator: Evaluator,
	numFolds: Int = 3,
//	estimator: PipelineStage,
	paramGridBuilder: ParamGridBuilder,
	transformers: Seq[PipelineStage],
	recipeName: String
) {


	/**
	  * Post-transformation dataset. This is the last stop before it's fed into the model
	  */
	lazy val transformedData: DataFrame = new Pipeline()
		.setStages(transformers.toArray)
		.fit(labeledData)
		.transform(labeledData)
		.cache()

	lazy val trainedModel: CrossValidatorModel = {
		val pipeline = new ParallelPipeline()
			.setStages(transformers.toArray)
//			.setStages(transformers.toArray :+ estimator)

		val cv = new ParallelCrossValidator()
			.setEstimator(pipeline)
			.setEvaluator(evaluator)
			.setEstimatorParamMaps(paramGridBuilder.build())
			.setNumFolds(numFolds)
			.setSeed(4) // chosen by fair dice roll. guaranteed to be random.

		cv.fit(labeledData)
	}

	lazy val accuracy: Double = {
		val predictions = trainedModel.transform(labeledData).cache()
		evaluator.evaluate(predictions)
	}

	/**
	  * This is the unlabeled dataset after predictions (ie looks like the labeled
	  * dataset because predictions fill in the missing labels)
	  */
	lazy val unlabeledPredictions: DataFrame = {
		val ts = System.currentTimeMillis
		val scoredUnlabeled = trainedModel
			.transform(unlabeledData.withColumn(labelCol, expr("0.0")))
			.drop(labelCol)
		// rename the predicted label col to the label col and drop all cols not present in
		// the original dataset (aka make the unlabeled dataset look like the labeled dataset)
		val srcCols = labeledData.schema.fields.map(_.name)
		val tgtCols = scoredUnlabeled.schema.fields.map(_.name) :+ labelCol
		val outCols = srcCols
			.filter(tgtCols.contains)
			.map(col)
		val conformed = scoredUnlabeled
			.withColumnRenamed(predictionCol, labelCol)
			.select(outCols.toSeq: _*)
		conformed
	}.cache()

	/**
	  * Write out the scored, unlabeled dataset to the specified
	  * path
	  * <p>
	  * fileName is composed of..
	  * <ul>
	  * <li> recipeName
	  * <li> accuracy
	  * <li> datetime created
	  * </ul>
	  * </p>
	  *
	  * @param prefix
	  */
	def writeCsv(prefix: Option[String] = None): Unit = {
		val df = new SimpleDateFormat("y-MMMd-hmma")
		val accStr = f"${accuracy.toFloat}%9.5f".trim
		val fileName = f"${recipeName}_${accStr}_${df.format(new Date())}"

		val outputPath = prefix
			.map(_ + s"/$fileName")
			.getOrElse(fileName)

		unlabeledPredictions
			.coalesce(1)
			.write
			.option("header", "true")
			.csv(outputPath)
	}




}
