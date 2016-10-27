package com.github.ceason.mllibextras.wowbot

import com.github.ceason.mllibextras.implicits._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import wowbot.gamedata.GameState
import org.apache.spark.ml.linalg.{Vector ⇒ MLVector}

/**
  *
  */
trait Workspace {

	val spark: SparkSession
	import spark.implicits._


	val actions: Dataset[ActionEvent]
	val gameStates: Dataset[GameState]
	val maxGap: Int


	val featurize: GameState ⇒ MLVector


	val actionWithGS: Dataset[(ActionEvent, GameState)] = {
		val bucketedGStates = gameStates.groupByKey(_.combatLogTimestamp / 1000)
		val bucketedActions = actions.groupByKey(_.timestamp.getTime / 1000)

		bucketedActions.cogroup(bucketedGStates){
			case (bucket, as, gs) ⇒

				// sort gstates newest to oldest
				val sortedGs = gs.toList.sortBy(-1 * _.combatLogTimestamp)

				// find the "first" gs that preceeded action (aka most recently preceeding)
				as.flatMap{ a ⇒
					sortedGs
						.find(_.combatLogTimestamp < a.timestamp.getTime)
						.map(a → _)
						.toList
				}
		}.cache()

	}

	val data: Dataset[FeaturizedData] = {
		actionWithGS.map{ case (a, gs) ⇒
			FeaturizedData(a, a.spellId, gs, featurize(gs))
		}
	}

	// Index labels, adding metadata to the label column.
	// Fit on whole dataset to include all labels in index.
	val labelIndexer = new StringIndexer()
		.setInputCol("spellId")
		.setOutputCol("indexedLabel")
		.fit(data)

	// Automatically identify categorical features, and index them.
	// Set maxCategories so features with > 4 distinct values are treated as continuous.
	val featureIndexer = new VectorIndexer()
		.setInputCol("features")
		.setOutputCol("indexedFeatures")
		.setMaxCategories(4)
		.fit(data)


	val Array(train, test) = actionWithGS.randomSplit(Array(0.7, 0.3))

	val rf = new RandomForestClassifier()
	    .setLabelCol("indexedLabel")
	    .setFeaturesCol("indexedFeatures")
	    .setNumTrees(10)

	// Convert indexed labels back to original labels.
	val labelConverter = new IndexToString()
		.setInputCol("prediction")
		.setOutputCol("predictedLabel")
		.setLabels(labelIndexer.labels)

	val pipeline = new Pipeline()
	    .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

	val model = pipeline.fit(train)

	val predictions = model.transform(test)

	predictions.select("predictedLabel", "spellId").show(5)

	val evaluator = new MulticlassClassificationEvaluator()
	    .setLabelCol("indexedLabel")
	    .setPredictionCol("prediction")
	    .setMetricName("accuracy")

	val accuracy = evaluator.evaluate(predictions)

	println(s"Test Error = ${1 - accuracy}")










}
