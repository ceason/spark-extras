package com.github.ceason.mllibextras

import com.github.ceason.mllibextras.transformer.HelloWorldTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FlatSpec

/**
  *
  */
class HelloWorldTransformerSpec extends FlatSpec
	with LocalSpark {

	import spark.implicits._


	"transform" should "add a new column" in {

		val df = spark.createDataset(Seq(
			"asdf" → 768,
			"bcd" → 54,
			"zzsdfd" → 45,
			"4asdfs" → 467
		)).toDF("randomText", "randomNumbers")

		val hwTransformer = new HelloWorldTransformer()


		val res = hwTransformer.transform(df)

		assert(res.schema.fields.length > df.schema.fields.length)

	}



}


