package com.github.ceason.mllibextras.kaggle

import scala.collection.JavaConverters._

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

}
