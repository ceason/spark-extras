package com.github.ceason.mllibextras.kaggle

import java.util.Properties

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
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
		"male"		→ expr("cast(if(sex=1,1,0) as double)"),
//		"sex"       → expr("coalesce(cast(sex       as double), 0.0)"),
		"education" → expr("coalesce(cast(education as double), 0.0)"),
		"marriage"  → expr("coalesce(cast(marriage  as double), 0.0)"),
		"age"       → expr("coalesce(cast(age       as double), 0.0)"),
		"pay_1"     → expr("coalesce(cast(pay_1     as double), 0.0)"),
//		"pay_2"     → expr("coalesce(cast(pay_2     as double), 0.0)"),
//		"pay_3"     → expr("coalesce(cast(pay_3     as double), 0.0)"),
//		"pay_4"     → expr("coalesce(cast(pay_4     as double), 0.0)"),
//		"pay_5"     → expr("coalesce(cast(pay_5     as double), 0.0)"),
//		"pay_6"     → expr("coalesce(cast(pay_6     as double), 0.0)"),
//		"bill_amt1" → expr("coalesce(cast(bill_amt1 as double), 0.0)"),
//		"bill_amt2" → expr("coalesce(cast(bill_amt2 as double), 0.0)"),
//		"bill_amt3" → expr("coalesce(cast(bill_amt3 as double), 0.0)"),
//		"bill_amt4" → expr("coalesce(cast(bill_amt4 as double), 0.0)"),
//		"bill_amt5" → expr("coalesce(cast(bill_amt5 as double), 0.0)"),
//		"bill_amt6" → expr("coalesce(cast(bill_amt6 as double), 0.0)"),
//		"pay_amt1"  → expr("coalesce(cast(pay_amt1  as double), 0.0)"),
//		"pay_amt2"  → expr("coalesce(cast(pay_amt2  as double), 0.0)"),
//		"pay_amt3"  → expr("coalesce(cast(pay_amt3  as double), 0.0)"),
//		"pay_amt4"  → expr("coalesce(cast(pay_amt4  as double), 0.0)"),
//		"pay_amt5"  → expr("coalesce(cast(pay_amt5  as double), 0.0)"),
//		"pay_amt6"  → expr("coalesce(cast(pay_amt6  as double), 0.0)"),
		"limit_bal" → expr("coalesce(cast(limit_bal as double), 0.0)")
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

		val cols = {
			col("customer_id") +:
			inputColExpr :+
			(col("default_oct") as 'label)
		}

		raw.select(cols :_*)
	}

	val trainPct: Double = 0.8

	val Array(training: DataFrame, testing: DataFrame) = {
		data.randomSplit(Array(trainPct, 1-trainPct), 12345)
	}


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
			.setMaxIter(100)
			.setRegParam(0.0)
			.setElasticNetParam(0.0)
		    .setTol(1e-6)
		    .setFitIntercept(true)
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
//			featureIndexer,
			labelIndexer,
			lr,
			labelConverter
		))

	val model: PipelineModel = pipeline.fit(training)


	val predictions: DataFrame = model.transform(testing)

	val evaluator: RegressionEvaluator = {
		new RegressionEvaluator()
			.setMetricName("rmse")
			.setLabelCol("indexedLabel")
		    .setPredictionCol("prediction")
	}

	val accuracy: Double = evaluator.evaluate(predictions)

	println(s" Chris accuracy: $accuracy")


	// dump to sql
	asDBCompatible(predictions)
		.write
		.mode(SaveMode.Overwrite)
		.jdbc("jdbc:postgresql://localhost/workspace?user=postgres&password=password", "predictions", new Properties)

	spark.stop()

}


object Workspace {


	def asDBCompatible(df: DataFrame): DataFrame = {
		val vec2arr: UserDefinedFunction = udf{ x: MLVector ⇒ x.toArray }

		val dbCompatibleColumns: Seq[Column] = {
			/*
			converting vectors to array
				- build select list by..
				- go over all cols in schema
				- if vector, do vec2arr
				- else pass through
	 		*/
			df.schema.fields.map{
				case StructField(name, dataType, _, _) ⇒ dataType match {
					case t if t.typeName == "vector" ⇒
						vec2arr(col(name)) as name
					case other ⇒
						col(name) as name
				}
			}
		}
		df.select(dbCompatibleColumns: _*)
	}


	def asDBCompatibleOld(cols: Seq[StructField]): Seq[Column] = {
		val vec2arr: UserDefinedFunction = udf{ x: MLVector ⇒ x.toArray }

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