package com.github.ceason.mllibextras.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  *
  */
class HelloWorldTransformer(
	val uid: String = Identifiable.randomUID("helloTransformer")
) extends Transformer {

	private val field = StructField("extraField", StringType)


	def transform(dataset: Dataset[_]): DataFrame = {

		val message = udf { () ⇒
			"hello, world!"
		}

		dataset.withColumn(field.name, message())
	}


	def transformSchema(schema: StructType): StructType = {
		StructType(schema.fields :+ field)
	}



	override def copy(extra: ParamMap): Transformer = {
		this
	}


}
