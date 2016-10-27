package com.github.ceason.mllibextras.wowbot

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.linalg.{Vector ⇒ MLVector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import wowbot.gamedata.GameState

/**
  *
  */
class DefaultFeaturizer(val uid: String) extends Transformer {

	// Empty constructor for reflective runtime instantiation from R/Python
	def this() = this(Identifiable.randomUID("DefaultFeaturizer"))


	override def transform(dataset: Dataset[_]): DataFrame = ???


	override def transformSchema(schema: StructType): StructType = ???



	override def copy(extra: ParamMap): Transformer = {
		val that = new DefaultFeaturizer(uid)
		copyValues(that, extra)
	}
}

