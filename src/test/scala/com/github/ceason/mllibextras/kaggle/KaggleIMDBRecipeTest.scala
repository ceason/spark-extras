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
			.na.fill("")
	}.cache()

	//	trainData.printSchema()

	val testData: DataFrame = spark.read
		.option("nullValue", "NA")
		.option("header", "true")
		.option("inferSchema", true)
		.csv("src/main/resources/imdb_test_public.csv")
		.na.fill(0)
		.na.fill("")
		.cache()

	val evaluatorCol = "prediction"
	val evaluator: RegressionEvaluator = new RegressionEvaluator()
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
		.setMaxBins(100)

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

	val stringCols: List[String] = List(
		//		"actor_1_name",
		//		"actor_2_name",
		//		"actor_3_name",
		//		"color",
		"content_rating",
		"country",
		//				"director_name",
		//		"genres",
		//		"plot_keywords",
		//		"movie_imdb_link",
		//		"movie_title",
		"language"
	)

	val splittableCols: List[String] = List(
		"director_name",
		"genres",
		"plot_keywords",
		//		"movie_imdb_link",
		"movie_title",
		"actor_1_name",
				"actor_2_name",
				"actor_3_name",
				"color"
	)

	val wordVectorizers: List[PipelineStage] = splittableCols.flatMap { col ⇒
		val splitter = new RegexTokenizer()
			.setInputCol(col)
			.setOutputCol(s"tokenized_$col")
			.setToLowercase(true)
			.setPattern("[\\s\\|]+")

		val hasher = new HashingTF()
			.setInputCol(s"tokenized_$col")
			.setOutputCol("tf_" + col)
			.setBinary(true)
			.setNumFeatures(256)

		splitter :: hasher :: Nil
	}
	val wordVectorCols: List[String] = splittableCols.map("tf_" + _)

	val strIndexers: List[StringIndexerModel] = stringCols.map { col ⇒
		new StringIndexer()
			.setInputCol(col)
			.setOutputCol(s"idx_$col")
			.fit(trainData.drop(labelCol) union testData)
	}
	val stringColsIndexed: List[String] = stringCols.map(col ⇒ s"idx_$col")

	val recipe1: KaggleRecipe = recipeTemplate("imdb_rfr",
		new ParamGridBuilder()
			//			.addGrid(rf.numTrees, 15 to 75 by 15)
			//			.addGrid(rf.featureSubsetStrategy, Seq("onethird", "sqrt", "log2"))
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

	val recipe2: KaggleRecipe = recipeTemplate("imdb_rfr",
		new ParamGridBuilder()
			.addGrid(rf.numTrees, 15 to 45 by 15)
			.addGrid(rf.featureSubsetStrategy, Seq("onethird", "sqrt", "log2"))
			.addGrid(rf.maxDepth, 3 to 9 by 3),
		wordVectorizers ++ strIndexers ++ List(
			new VectorAssembler()
				.setInputCols((numericCols ++
					stringColsIndexed ++
					wordVectorCols
					).toArray)
				.setOutputCol("selectedFeatures")
			//			,new VectorIndexer()
			//				.setInputCol("features")
			//				.setOutputCol("indexedFeatures")
			//				.setMaxCategories(4)
			//			, new ChiSqSelector()
			//				.setNumTopFeatures(22)
			//				.setFeaturesCol("features")
			//				.setLabelCol(labelCol)
			//				.setOutputCol("selectedFeatures")
			, rf
		))

	val start = System.currentTimeMillis

	//	recipe2.transformedData.printSchema()
	//	recipe2.transformedData.show()
	recipe2.writeCsv("id", "imdb_score as imdb_score_yhat")

	val duration = (System.currentTimeMillis - start) / 1000d
	println(s"Total time seconds: $duration")
}
