package com.github.ceason.mllibextras.kaggle

import com.github.ceason.mllibextras.LocalSpark
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector ⇒ MLVector}
import org.apache.spark.ml.regression.{DecisionTreeRegressor, RandomForestRegressor}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
  *
  */
class KaggleIMDBRecipeTest extends FlatSpec with LocalSpark {


	// this is where common stuff would go (datasets, target column etc)

	val totallyRandomSeed = 4

	val labelCol = "imdb_score"

	val trainData: DataFrame = {
		spark.read
			.option("nullValue", "NA")
			.option("header", "true")
			.option("inferSchema", true)
			.csv("src/main/resources/imdb_train_public.csv")
			.na.fill(0)
	}.cache()

	//	trainData.printSchema()

	val testData: DataFrame = spark.read
		.option("nullValue", "NA")
		.option("header", "true")
		.option("inferSchema", true)
		.csv("src/main/resources/imdb_test_public.csv")
		.na.fill(0)
		.cache()

	val evaluatorCol = "prediction"
	val evaluator = new RegressionEvaluator()
		.setMetricName("rmse")
		.setPredictionCol(evaluatorCol)
		.setLabelCol(labelCol)

	val recipeTemplate: (String, ParamGridBuilder, Seq[PipelineStage]) ⇒ KaggleRecipe = {
		KaggleRecipe(
			labeledData = trainData,
			unlabeledData = testData,
			labelCol = labelCol,
			predictionCol = evaluatorCol,
			evaluator = evaluator,
			numFolds = 3,
			_, _, _)
	}

	// here is where custom stuff would go (different models, pipeline steps etc)


	spark.udf.register("vec2arr", (x: MLVector) ⇒ x.toArray)

	val rf: RandomForestRegressor = new RandomForestRegressor()
		.setSeed(totallyRandomSeed)
		.setFeaturesCol("selectedFeatures")
		.setLabelCol(labelCol)
		.setPredictionCol(evaluatorCol)

	val dt = new DecisionTreeRegressor()
		.setLabelCol(labelCol)
		.setFeaturesCol("indexedFeatures")

	val numericCols: List[String] = List("actor_1_facebook_likes",
		"actor_2_facebook_likes",
		"actor_3_facebook_likes",
		"budget",
		"cast_total_facebook_likes",
		"director_facebook_likes",
		"duration",
		"facenumber_in_poster",
		"gross",
		"movie_facebook_likes",
		"num_critic_for_reviews",
		"num_user_for_reviews",
		"num_voted_users",
		"title_year",
		"aspect_ratio")

	val stringCols: List[String] = List("actor_1_name",
		"actor_2_name",
		"actor_3_name",
		"color",
		"content_rating",
		"country",
		"director_name",
		"genres",
		"language",
		"movie_imdb_link",
		"movie_title",
		"plot_keywords")

	val recipe1: KaggleRecipe = recipeTemplate("imdb_rfr",
		new ParamGridBuilder()
			.addGrid(rf.numTrees, 15 to 75 by 15)
			.addGrid(rf.featureSubsetStrategy, Seq("onethird", "sqrt", "log2"))
			.addGrid(rf.maxDepth, 3 to 9 by 3),
		List(
			new VectorAssembler()
				.setInputCols(numericCols.toArray)
				.setOutputCol("features"),
			new VectorIndexer()
				.setInputCol("features")
				.setOutputCol("indexedFeatures")
				.setMaxCategories(4),
			new ChiSqSelector()
				.setNumTopFeatures(22)
				.setFeaturesCol("indexedFeatures")
				.setLabelCol(labelCol)
				.setOutputCol("selectedFeatures"),
			rf
		))

	val start = System.currentTimeMillis

	recipe1.transformedData.printSchema()
	recipe1.transformedData.show(false)
	recipe1.writeCsv()

	val duration = (System.currentTimeMillis - start) / 1000d
	println(s"Total time seconds: $duration")
}
