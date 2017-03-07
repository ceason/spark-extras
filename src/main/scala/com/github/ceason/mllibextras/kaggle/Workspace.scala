package com.github.ceason.mllibextras.kaggle

import java.util.Properties

import com.github.ceason.mllibextras.LogLossEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.ml.linalg.{Vector ⇒ MLVector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}

/**
  *
  */
trait Workspace {
	import Workspace._

	val spark: SparkSession = {
		SparkSession
			.builder
			.appName("kaggle")
			.master("local[6]")
			.getOrCreate()
	}

	val featureExprs: Map[String, Column] = Map(
		"sex"       → expr("coalesce(cast(sex       as double), 0.0)"),
		"education" → expr("coalesce(cast(education as double), 0.0)"),
		"marriage"  → expr("coalesce(cast(marriage  as double), 0.0)"),
		"age"       → expr("coalesce(cast(age       as double), 0.0)"),
		"age_group" → expr("cast((case " +
						   "when age > 62 then 1 " +
						   "when age > 30 then 2 " +
						   "else 0 " +
						   "end) as double)"),
		"pay_1"     → expr("coalesce(cast(pay_1     as double), 0.0)"),
		"pay_2"     → expr("coalesce(cast(pay_2     as double), 0.0)"),
		"pay_3"     → expr("coalesce(cast(pay_3     as double), 0.0)"),
		"pay_4"     → expr("coalesce(cast(pay_4     as double), 0.0)"),
		"pay_5"     → expr("coalesce(cast(pay_5     as double), 0.0)"),
		"pay_6"     → expr("coalesce(cast(pay_6     as double), 0.0)"),
		"bill_amt1" → expr("coalesce(cast(bill_amt1 as double), 0.0)"),
		"bill_amt2" → expr("coalesce(cast(bill_amt2 as double), 0.0)"),
		"bill_amt3" → expr("coalesce(cast(bill_amt3 as double), 0.0)"),
		"bill_amt4" → expr("coalesce(cast(bill_amt4 as double), 0.0)"),
		"bill_amt5" → expr("coalesce(cast(bill_amt5 as double), 0.0)"),
		"bill_amt6" → expr("coalesce(cast(bill_amt6 as double), 0.0)"),
		"pay_amt1"  → expr("coalesce(cast(pay_amt1  as double), 0.0)"),
		"pay_amt2"  → expr("coalesce(cast(pay_amt2  as double), 0.0)"),
		"pay_amt3"  → expr("coalesce(cast(pay_amt3  as double), 0.0)"),
		"pay_amt4"  → expr("coalesce(cast(pay_amt4  as double), 0.0)"),
		"pay_amt5"  → expr("coalesce(cast(pay_amt5  as double), 0.0)"),
		"pay_amt6"  → expr("coalesce(cast(pay_amt6  as double), 0.0)"),
		"limit_bal" → expr("coalesce(cast(limit_bal as double), 0.0)")
	)

	val featureCols: Array[String] = featureExprs.keys.toArray

	val inputColExpr: Seq[Column] = featureExprs.toSeq.map{case (name, ex) ⇒ ex.as(name)}

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

		raw.select(cols :_*).repartition(4).cache
	}

	val unlabeledData: DataFrame = {
		spark.read
			.option("nullValue", "NA")
			.option("header", true)
			.option("inferSchema", true)
			.csv("src/main/resources/test.csv")
			.select(col("customer_id") +: inputColExpr :_*)
	}

	val assembler: VectorAssembler = {
		new VectorAssembler()
			.setInputCols(featureCols)
			.setOutputCol("features")
	}

	val featureIndexer: VectorIndexer = {
		new VectorIndexer()
			.setInputCol("features")
			.setOutputCol("indexedFeatures")
			.setMaxCategories(4)
	}

	val selector: ChiSqSelector = {
		new ChiSqSelector()
			.setNumTopFeatures(22)
			.setFeaturesCol("indexedFeatures")
			.setLabelCol("indexedLabel")
			.setOutputCol("selectedFeatures")
	}

	val labelIndexer: StringIndexerModel = {
		new StringIndexer()
			.setInputCol("label")
			.setOutputCol("indexedLabel")
			.fit(data)
	}

	val rf: RandomForestClassifier = {
		new RandomForestClassifier()
			.setSeed(12345)
			.setFeaturesCol("selectedFeatures")
			.setLabelCol("indexedLabel")
			.setPredictionCol("prediction")
	}

	val labelConverter: IndexToString = {
		new IndexToString()
			.setInputCol("prediction")
			.setOutputCol("predictedLabel")
			.setLabels(labelIndexer.labels)
	}

	spark.udf.register("vec2arr", (x: MLVector) ⇒ x.toArray)

	val st: SQLTransformer = {
		new SQLTransformer()
			.setStatement("select *, vec2arr(probability)[1] as prob from __THIS__")
	}

	val pipeline: Pipeline = {
		new Pipeline()
			.setStages(Array(
				assembler,
				featureIndexer,
				labelIndexer,
				selector,
				rf,
				labelConverter,
				st
			))
	}

	val rfParamGrid: Array[ParamMap] = {
		new ParamGridBuilder()
			.addGrid(rf.numTrees, 15 to 75 by 15)
			.addGrid(rf.featureSubsetStrategy, Seq("onethird", "sqrt", "log2"))
			.addGrid(rf.impurity, Seq("entropy", "gini"))
			.addGrid(rf.maxDepth, 3 to 9 by 2)
			.build()
	}

	val evaluator: LogLossEvaluator = {
		new LogLossEvaluator()
			.setLabelCol("indexedLabel")
			.setProbabilityCol("prob")
	}

	val cv: CrossValidator = {
		new CrossValidator()
			.setEstimator(pipeline)
			.setEvaluator(evaluator)
			.setEstimatorParamMaps(rfParamGrid)
			.setNumFolds(3)
			.setSeed(12345)
	}

	val model: CrossValidatorModel = cv.fit(data)

	val predictions: DataFrame = model.transform(data)

	val accuracy: Double = evaluator.evaluate(predictions)

	val pctCorrect: Double = {
		1.0 * predictions.filter("indexedLabel = prediction").count() / predictions.count()
	}

	println(s"      % correct: $pctCorrect")
	println(s" Chris accuracy: $accuracy")

	val unlabeledPredictions: DataFrame = {
		val ts = System.currentTimeMillis()
		model
			.transform(unlabeledData)
			.select("customer_id", "prob")
			.withColumn("log_loss", lit(accuracy))
			.withColumn("tstamp", lit(ts))
	}

	// dump to sql
	val localSql = "jdbc:postgresql://localhost/workspace?user=postgres&password=password"
	asDBCompatible(unlabeledPredictions)
		.write.mode(SaveMode.Append)
		.jdbc(localSql, "unlabeled_predictions", new Properties)

	asDBCompatible(predictions)
		.write.mode(SaveMode.Overwrite)
		.jdbc(localSql, "predictions", new Properties)

	spark.stop()

}


object Workspace {

	val vec2arr: UserDefinedFunction = udf{ x: MLVector ⇒ x.toArray }

	def asDBCompatible(df: DataFrame): DataFrame = {

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

}