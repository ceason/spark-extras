package com.github.ceason.mllibextras.wowbot

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.linalg.{Vector ⇒ MLVector}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.functions._
import wowbot.gamedata.GameState

/**
  *
  */
class DefaultFeaturizer(val uid: String) extends Transformer {

	// Empty constructor for reflective runtime instantiation from R/Python
	def this() = this(Identifiable.randomUID("DefaultFeaturizer"))


	// parameters
	val inputCol : Param[String] = new Param(this, "inputCol", "input column name")
	val outputCol: Param[String] = new Param(this, "outputCol", "column name to put features")
	val features : Param[Array[String]] = new Param(this, "features", "features to select")


	def setInputCol(value: String): this.type = set(inputCol, value)
	def setOutputCol(value: String): this.type = set(outputCol, value)
	def setFeatures(selected: Array[String]): this.type = set(features, selected)

	def getFeatures(): Array[String] = $(features)
	def getOutputCol(): String = $(outputCol)
	def getInputCol(): String = $(inputCol)


	override def transform(dataset: Dataset[_]): DataFrame = {

		/*
			..but will it run the featurizer twice then???.......

				- maybe have one col of Array[Feature] and *then* "unpivot" to a struct?
					^ because selection of feature fields must happen only at training time

				- `Feature` specific unpivot? or generic??
					^ leaning toward specific. generic functionality already in api


				- ..how to chain in/out fields together nicely?



			how get features out of dataset before pipeline is built???
				-


			how want to interact w/ this thing??
				-
		 */

		val selected = $(features)

		val xform = udf { gs: GameState ⇒
			val all = DefaultFeaturizer.featurize(gs)
			selected.map{ name ⇒
				all.find(_.name == name)
					.map(_.value)
			}
		}

		dataset.withColumn($(outputCol), xform(col($(inputCol))))
	}

	override def transformSchema(schema: StructType): StructType = {
		// TODO: validate input col
		val fields = $(features).map(name ⇒ StructField(name, DoubleType))
		val outField = StructField($(outputCol), StructType(fields))
		StructType(schema.fields :+ outField)
	}

	override def copy(extra: ParamMap): Transformer = {
		val that = new DefaultFeaturizer(uid)
		copyValues(that, extra)
	}
}

object DefaultFeaturizer {

	def featurize(gs: GameState): Array[Feature] = {

		???
	}

	def fromDS(ds: Dataset[GameState]): DefaultFeaturizer = {
		// pull out unique feature names
		val featureNames = ds
			.flatMap(gs ⇒ featurize(gs).map(_.name))
			.distinct()
			.collect()

		new DefaultFeaturizer()
		    .setFeatures(featureNames)
	}
}