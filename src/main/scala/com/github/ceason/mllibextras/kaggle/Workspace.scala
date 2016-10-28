package com.github.ceason.mllibextras.kaggle

import java.util.Properties

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.ml.linalg.{Vector ⇒ MLVector}

/**
  *
  */
//noinspection TypeAnnotation
trait Workspace {
	import Workspace._


	val spark: SparkSession = SparkSession
		.builder
		.appName("kaggle")
		.master("local[*]")
		.getOrCreate()

	val featureExprs: Map[String, Column] = Map(
		"sex"       → expr("cast(coalesce(sex      , 0) as double)"),
		"education" → expr("cast(coalesce(education, 0) as double)"),
		"marriage"  → expr("cast(coalesce(marriage , 0) as double)"),
		"age"       → expr("cast(coalesce(age      , 0) as double)"),
		"pay_1"     → expr("cast(coalesce(pay_1    , 0) as double)"),
//		"pay_2"     → expr("cast(coalesce(pay_2    , 0) as double)"),
//		"pay_3"     → expr("cast(coalesce(pay_3    , 0) as double)"),
//		"pay_4"     → expr("cast(coalesce(pay_4    , 0) as double)"),
//		"pay_5"     → expr("cast(coalesce(pay_5    , 0) as double)"),
//		"pay_6"     → expr("cast(coalesce(pay_6    , 0) as double)"),
//		"bill_amt1" → expr("cast(coalesce(bill_amt1, 0) as double)"),
//		"bill_amt2" → expr("cast(coalesce(bill_amt2, 0) as double)"),
//		"bill_amt3" → expr("cast(coalesce(bill_amt3, 0) as double)"),
//		"bill_amt4" → expr("cast(coalesce(bill_amt4, 0) as double)"),
//		"bill_amt5" → expr("cast(coalesce(bill_amt5, 0) as double)"),
//		"bill_amt6" → expr("cast(coalesce(bill_amt6, 0) as double)"),
//		"pay_amt1"  → expr("cast(coalesce(pay_amt1 , 0) as double)"),
//		"pay_amt2"  → expr("cast(coalesce(pay_amt2 , 0) as double)"),
//		"pay_amt3"  → expr("cast(coalesce(pay_amt3 , 0) as double)"),
//		"pay_amt4"  → expr("cast(coalesce(pay_amt4 , 0) as double)"),
//		"pay_amt5"  → expr("cast(coalesce(pay_amt5 , 0) as double)"),
//		"pay_amt6"  → expr("cast(coalesce(pay_amt6 , 0) as double)"),
		"limit_bal" → expr("cast(coalesce(limit_bal, 0) as double)")
	)

	val featureCols: Array[String] = featureExprs.keys.toArray


	val inputColExpr: Seq[Column] = featureExprs.toSeq.map{case (name, ex) ⇒ ex.as(name)}

	// $example on$
	// Load training data
	val data: DataFrame = {
		val raw = spark.read
			.option("nullValue", "NA")
			.option("header", true)
			.option("inferSchema", true)
			.csv("src/main/resources/train.csv")

		val cols = inputColExpr :+ (col("default_oct") as 'label)

		raw.select(cols :_*)
	}

	val Array(training: DataFrame, testing: DataFrame) = data.randomSplit(Array(0.7, 0.3))


	val unlabeledData: DataFrame = {
		spark.read
			.option("nullValue", "NA")
			.option("header", true)
			.option("inferSchema", true)
			.csv("src/main/resources/test.csv")
			.select(inputColExpr :_*)
	}


	val assembler: VectorAssembler = {
		new VectorAssembler()
			.setInputCols(featureCols)
			.setOutputCol("features")
	}


	val featureIndexer: VectorIndexerModel = {
		new VectorIndexer()
			.setInputCol("features")
			.setOutputCol("indexedFeatures")
			.setMaxCategories(4)
			.fit(assembler.transform(data))
	}


	val labelIndexer: StringIndexerModel = {
		new StringIndexer()
			.setInputCol("label")
			.setOutputCol("indexedLabel")
			.fit(data)
	}


	val lr: LogisticRegression = {
		new LogisticRegression()
//			.setFeaturesCol("indexedFeatures")
			.setFeaturesCol("features")
			.setLabelCol("indexedLabel")
			.setPredictionCol("prediction")
			.setMaxIter(1000)
			.setRegParam(0.3)
			.setElasticNetParam(0.8)
	}

	val labelConverter: IndexToString = {
		new IndexToString()
			.setInputCol("prediction")
			.setOutputCol("predictedLabel")
			.setLabels(labelIndexer.labels)
	}

	val pipeline: Pipeline = new Pipeline()
	    .setStages(Array(
			assembler,
			featureIndexer,
			labelIndexer,
			lr,
			labelConverter
		))

	val model: PipelineModel = pipeline.fit(training)


	val predictions: DataFrame = model.transform(testing)

	val evaluator: BinaryClassificationEvaluator = new BinaryClassificationEvaluator()
		.setLabelCol("indexedLabel")

	val accuracy: Double = evaluator.evaluate(predictions)

	println(s"accuracy: $accuracy")







	// dump to sql
	predictions.coalesce(1)
		.select(asDBCompatible(predictions.schema.fields) :_*)
//		.drop("features", "indexedFeatures", "rawPrediction", "probability")
//		.withColumn("testArray", array(lit(1), lit(2), lit(3)))
		.write
		.mode(SaveMode.Overwrite)
		.jdbc("jdbc:postgresql://localhost/workspace?user=postgres&password=password", "predictions", new Properties)
//		.jdbc("jdbc:sqlite:x.db", "predictions", new Properties)

	spark.stop()

}


object Workspace {


	def asDBCompatible(cols: Seq[StructField]): Seq[Column] = {
		val vec2arr: UserDefinedFunction = udf{ x: MLVector ⇒ x.toArray }
		/*
			converting vectors to array
				- build select list by..
				- go over all cols in schema
				- if vector, do vec2arr
				- else pass through
	 	*/
		cols.map{
			case StructField(name, dataType, _, _) ⇒ dataType match {
				case t if t.typeName == "vector" ⇒
					vec2arr(col(name)) as name
				case other ⇒
					col(name) as name
			}
		}
	}
}