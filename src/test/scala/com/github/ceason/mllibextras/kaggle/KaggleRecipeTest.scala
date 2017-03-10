package com.github.ceason.mllibextras.kaggle

import com.github.ceason.mllibextras.{LocalSpark, LogLossEvaluator}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
  *
  */
class KaggleRecipeTest extends FlatSpec with LocalSpark {


	// this is where common stuff would go (datasets, target column etc)

	val totallyRandomSeed = 4

	val labelCol = "default_oct"

	val trainData: DataFrame = spark.read
		.option("nullValue", "NA")
		.option("header", "true")
		.option("inferSchema", true)
		.csv("src/main/resources/train.csv")

	val testData: DataFrame = spark.read
		.option("nullValue", "NA")
		.option("header", "true")
		.option("inferSchema", true)
		.csv("src/main/resources/test.csv")


	// here is where custom stuff would go (different models, pipeline steps etc)

	val rf: RandomForestClassifier = new RandomForestClassifier()
		.setSeed(totallyRandomSeed)
		.setFeaturesCol("selectedFeatures")
		.setLabelCol("indexedLabel")
		.setPredictionCol("prediction")

	val recipe1: KaggleRecipe = KaggleRecipe(
		labeledData = trainData,
		unlabeledData = testData,
		labelCol = labelCol,
		predictionCol = "predict_default_oct",
		evaluator = new LogLossEvaluator()
			.setLabelCol("default_oct")
			.setProbabilityCol("predict_default_oct"),
		numFolds = 2,
		estimator = rf,
		paramGridBuilder = new ParamGridBuilder()
			.addGrid(rf.numTrees, 15 to 75 by 15)
			.addGrid(rf.featureSubsetStrategy, Seq("onethird", "sqrt", "log2"))
			.addGrid(rf.impurity, Seq("entropy", "gini"))
			.addGrid(rf.maxDepth, 3 to 9 by 2),

		recipeName = "someTestRecipe",

		transformers = List(
			new SQLTransformer().setStatement(
				"""select
	   				customer_id,
					coalesce(cast(pay_1     as double), 0.0) as pay_1,
					coalesce(cast(pay_2     as double), 0.0) as pay_2,
					coalesce(cast(pay_3     as double), 0.0) as pay_3,
					coalesce(cast(pay_4     as double), 0.0) as pay_4,
					coalesce(cast(pay_5     as double), 0.0) as pay_5,
					coalesce(cast(pay_6     as double), 0.0) as pay_6,
					coalesce(cast(bill_amt1 as double), 0.0) as bill_amt1,
					coalesce(cast(bill_amt2 as double), 0.0) as bill_amt2,
					coalesce(cast(bill_amt3 as double), 0.0) as bill_amt3,
					coalesce(cast(bill_amt4 as double), 0.0) as bill_amt4,
					coalesce(cast(bill_amt5 as double), 0.0) as bill_amt5,
					coalesce(cast(bill_amt6 as double), 0.0) as bill_amt6,
					coalesce(cast(pay_amt1  as double), 0.0) as pay_amt1,
					coalesce(cast(pay_amt2  as double), 0.0) as pay_amt2,
					coalesce(cast(pay_amt3  as double), 0.0) as pay_amt3,
					coalesce(cast(pay_amt4  as double), 0.0) as pay_amt4,
					coalesce(cast(pay_amt5  as double), 0.0) as pay_amt5,
					coalesce(cast(pay_amt6  as double), 0.0) as pay_amt6,
					coalesce(cast(limit_bal as double), 0.0) as limit_bal,
	 				default_oct
			 	from __THIS__"""),
			new VectorAssembler()
				.setInputCols(Array("pay_1", "pay_2", "pay_3", "pay_4", "pay_5", "pay_6",
					"bill_amt1", "bill_amt2", "bill_amt3", "bill_amt4", "bill_amt5", "bill_amt6",
					"pay_amt1", "pay_amt2", "pay_amt3", "pay_amt4", "pay_amt5", "pay_amt6",
					"limit_bal"))
				.setOutputCol("features"),
			new VectorIndexer()
				.setInputCol("features")
				.setOutputCol("indexedFeatures")
				.setMaxCategories(4),
			new StringIndexer()
				.setInputCol("default_oct")
				.setOutputCol("indexedLabel")
				.fit(trainData),
			new ChiSqSelector()
				.setNumTopFeatures(22)
				.setFeaturesCol("indexedFeatures")
				.setLabelCol("indexedLabel")
				.setOutputCol("selectedFeatures")
		)

	)

//	recipe1.transformedData.printSchema()
	recipe1.transformedData.show()
//	recipe1.writeCsv()


}
