package com.github.ceason.mllibextras.kaggle

import java.util.Properties

import com.github.ceason.mllibextras.LogLossEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
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
//noinspection TypeAnnotation
trait Workspace {
	import Workspace._


	val spark: SparkSession = SparkSession
		.builder
		.appName("kaggle")
		.master("local[6]")
		.getOrCreate()

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

		raw.select(cols :_*).repartition(4)
	}.cache

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
			.select(col("customer_id") +: inputColExpr :_*)
	}


	val assembler: VectorAssembler = {
		new VectorAssembler()
			.setInputCols(featureCols)
			.setOutputCol("features")
	}


	val featureIndexer = {
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

	val pca: PCA = {
		new PCA()
			.setInputCol("indexedFeatures")
			.setOutputCol("selectedFeatures")
			.setK(15)
	}



	val labelIndexer: StringIndexerModel = {
		new StringIndexer()
			.setInputCol("label")
			.setOutputCol("indexedLabel")
			.fit(data)
	}


	val lr: LogisticRegression = {
		new LogisticRegression()
			.setFeaturesCol("selectedFeatures")
//			.setFeaturesCol("features")
			.setLabelCol("indexedLabel")
			.setPredictionCol("prediction")
			.setMaxIter(100)
			.setRegParam(0.0)
			.setElasticNetParam(0.0)
		    .setTol(1e-6)
		    .setFitIntercept(true)
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

	val pipeline: Pipeline = new Pipeline()
	    .setStages(Array(
			assembler,
			featureIndexer,
			labelIndexer,
			selector,
//			pca,
			rf, // 0.3917766448774857,
//			lr, // 0.4662163397557657
			labelConverter,
			st
		))


	val rfParamGrid: Array[ParamMap] = {
		new ParamGridBuilder()
			.addGrid(rf.numTrees, 15 to 75 by 15)
			.addGrid(rf.featureSubsetStrategy, Seq("onethird", "sqrt", "log2"))
			.addGrid(rf.impurity, Seq("entropy", "gini"))
			.addGrid(rf.maxDepth, 3 to 9 by 2)
			.build()
	}

	val lrParamGrid: Array[ParamMap] = {
		new ParamGridBuilder()
			.addGrid(lr.regParam, 0.0 to 0.5 by 0.05)
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
//			.setEstimatorParamMaps(lrParamGrid)
			.setNumFolds(3)
			.setSeed(12345)
	}



	val begin = System.currentTimeMillis()

	val model: CrossValidatorModel = {
		cv.fit(data) // 0.389063931892257 639s, 563s

	}
	//	val model: PipelineModel = pipeline.fit(training) // 0.43864579472730203

	val duration = (System.currentTimeMillis() - begin) / 1000
	println(s"Total duration: $duration seconds")






	val predictions: DataFrame = model
		.transform(data) // 0.3954236128538358



	val accuracy: Double = evaluator.evaluate(predictions)

	val pctCorrect: Double = {
		1.0 * predictions.filter("indexedLabel = prediction").count() / predictions.count()
	}

	println(s"      % correct: $pctCorrect")
	println(s" Chris accuracy: $accuracy")

	val unlabeledPredictions: DataFrame = {
		model
			.transform(unlabeledData)
			.select("customer_id", "prob")
		    .withColumn("log_loss", lit(accuracy))
	}

	asDBCompatible(unlabeledPredictions).write
		.mode(SaveMode.Append)
		.jdbc("jdbc:postgresql://localhost/workspace?user=postgres&password=password", "unlabeled_predictions", new Properties)


	// dump to sql
	asDBCompatible(predictions)
		.write
		.mode(SaveMode.Overwrite)
		.jdbc("jdbc:postgresql://localhost/workspace?user=postgres&password=password", "predictions", new Properties)

	spark.stop()



}


object Workspace {


	def time[R](f: ⇒ R): R ={
		val begin = System.currentTimeMillis()
		val ret = f
		val duration = (System.currentTimeMillis() - begin) / 1000
		println(s"Total duration: $duration seconds")
		ret
	}

	val vec2arr: UserDefinedFunction = udf{ x: MLVector ⇒ x.toArray }


	val vec: UserDefinedFunction = udf{ (i: Int, v: MLVector) ⇒ v(i) }

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