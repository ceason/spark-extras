package com.github.ceason.mllibextras.kaggle

import java.text.SimpleDateFormat
import java.util.Date

import com.github.ceason.mllibextras.ParallelPipeline
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  *
  */
trait KaggleTpl {

	/*
		Kaggle commonalities (user provides)..
			- 2 datasets: labeled and unlabeled
			- evaluator
			- Pipeline
			- ParamGridBuilder
			- (numFolds?)

		Template Process
			- build crossvalidator from paramgrid+pipeline+evaluator
			- fit labeled data to crossvalidator
			- apply resultant model to unlabeled data
			- save scored unlabeled (and labeled?) data to path
				- include in filename:
					- "pipeline name"
					- score
					- datetime created
					- (evaluator type?)

		Using it..
			- create single instance of each kind of transformer or model that we use
			- then compose multiple 'full thing' by using
				- pipeline of aforementioned transformers/models
				- gridsearch params
				- 'name' for this composition

		Bonus:
			- parallelize crossvalidator by..
				- extending and overriding 'fit' ?
				- ^ acutally, it's the estimator that needs to be parallelized
	 */

	/*

		right now goals:
			- define/swap different pipelines easily
			- print "post transform dataset"
			- save scored-unlabeled dataset to file per above convention
	 */


}


