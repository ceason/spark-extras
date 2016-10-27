package com.github.ceason.mllibextras.wowbot

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

/**
  *
  */
class FeatureUnpivoter(val uid: String) extends Transformer {

	// Empty constructor for reflective runtime instantiation from R/Python
	def this() = this(Identifiable.randomUID("FeatureUnpivoter"))

	// params
	val inputCol : Param[String] = new Param(this, "inputCol", "input column name")
	val outputCol: Param[String] = new Param(this, "outputCol", "column name to put features")
	val features : Param[Seq[String]] = new Param(this, "features", "features to select")


	def transform(dataset: Dataset[_]): DataFrame = {



	}


	def transformSchema(schema: StructType): StructType = {


		???
	}


	override def copy(extra: ParamMap): Transformer = {
		val that = new DefaultFeaturizer(uid)
		copyValues(that, extra)
	}


}
