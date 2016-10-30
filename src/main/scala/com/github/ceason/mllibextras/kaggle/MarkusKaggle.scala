package com.github.ceason.mllibextras.kaggle
import java.util.Properties

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, RegressionMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable
/**
  * Created by mbraasch on 10/27/16.
  */
object MarkusKaggle {

	case class R(customerId: Int, limitBal: Int, male: Int, female: Int, education: Int, marriage: Int, age: Int,
		pay1: Int, pay2: Int, pay3: Int, pay4: Int, pay5: Int, pay6: Int,
		billAmt1: Int, billAmt2: Int, billAmt3: Int, billAmt4: Int, billAmt5: Int, billAmt6: Int,
		payAmt1: Int, payAmt2: Int, payAmt3: Int, payAmt4: Int, payAmt5: Int, payAmt6: Int, defaultOct: Int, defaultRaw: String)

	case class Params(
		input: String           = null,
		testInput: String       = "",
		dataFormat: String      = "libsvm",
		regParam: Double        = 0.0,
		elasticNetParam: Double = 0.0,
		maxIter: Int            = 100,
		fitIntercept: Boolean   = true,
		tol: Double             = 1E-6,
		fracTest: Double        = 0.2
	)

	val spark: SparkSession = {
		SparkSession
			.builder
			.appName("kaggle")
			.master("local[*]")
			.getOrCreate()
	}

	def getInt(str: Any) = {
		str.toString match {
			case "NA" => 0
			case x => x.toDouble.toInt
		}
	}

	def runJob() = {
		val data = spark.read.option("sep", ",").option("header", "true").csv("src/main/resources/train.csv")
		
		import spark.implicits._

		val rs = {
			data.map{x ⇒
				R(
					customerId = getInt(x(0)),
					limitBal   = getInt(x(1)),
					male       = if (getInt(x(2)) == 1) 1 else 0,
					female     = if (getInt(x(2)) == 2) 1 else 0,
					education  = getInt(x(3)),
					marriage   = getInt(x(4)),
					age        = getInt(x(5)),
					pay1       = getInt(x(6)),
					pay2       = getInt(x(7)),
					pay3       = getInt(x(8)),
					pay4       = getInt(x(9)),
					pay5       = getInt(x(10)),
					pay6       = getInt(x(11)),
					billAmt1   = getInt(x(12)),
					billAmt2   = getInt(x(13)),
					billAmt3   = getInt(x(14)),
					billAmt4   = getInt(x(15)),
					billAmt5   = getInt(x(16)),
					billAmt6   = getInt(x(17)),
					payAmt1    = getInt(x(18)),
					payAmt2    = getInt(x(19)),
					payAmt3    = getInt(x(20)),
					payAmt4    = getInt(x(21)),
					payAmt5    = getInt(x(22)),
					payAmt6    = getInt(x(23)),
					defaultOct = if (x(24).toString == "yes") 1 else 0,
					defaultRaw = x(24).toString
				)
			}
		}

		val categoricalVariables = Array("limitBal","male", "education", "marriage", "age", "pay1")

		val categoricalIndexers: Array[PipelineStage] = {
			categoricalVariables.map( i => new StringIndexer()
				.setInputCol(i).setOutputCol(i+"Index").setHandleInvalid("skip") )
		}

		val labelIndexer = {
			new StringIndexer()
				.setInputCol("defaultOct")
				.setOutputCol("indexedLabel")
		}

		val categoricalEncoders: Array[PipelineStage] = {
			categoricalVariables.map(e => new OneHotEncoder()
				.setInputCol(e + "Index").setOutputCol(e + "Vec"))
		}

		val assembler = {
			new VectorAssembler()
				.setInputCols( Array(
					"limitBal", "male", "education", "marriage", "age","pay1"))
				//"limitBalVec", "maleVec", "educationVec", "marriageVec", "ageVec","pay1Vec"))
				.setOutputCol("features")
		}

		val params = Params()

		// Run training algorithm to build the model
		val lr = {
			new LogisticRegression()
				//.setLabelCol("indexedLabel")
				.setLabelCol("defaultOct")
				.setFeaturesCol("features")
				.setRegParam(params.regParam)
				.setElasticNetParam(params.elasticNetParam)
				.setMaxIter(params.maxIter)
				.setTol(params.tol)
				.setFitIntercept(params.fitIntercept)
		}

		val steps = Array(assembler, lr)

		val pipeline = new Pipeline()
			.setStages(steps)

		// Fit the Pipeline.
		val Array(training, test) = rs.randomSplit(Array(0.8, 0.2), seed = 12345)
		val model = pipeline.fit(training)


		val predictions = model.transform(test)
		val evaluator = {
			new RegressionEvaluator()
				.setMetricName("rmse")
				.setLabelCol("defaultOct")
				.setPredictionCol("prediction")
		}

		val rmse = evaluator.evaluate(predictions)

		val holdout = {
			predictions
				.select("prediction", "defaultOct")
				.orderBy(abs(col("prediction")-col("defaultOct")))
		}


//		val rm = new RegressionMetrics(holdout.rdd.map(x ⇒ (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

		import Workspace._
		asDBCompatible(predictions)
			.write
			.mode(SaveMode.Overwrite)
			.jdbc("jdbc:postgresql://localhost/workspace?user=postgres&password=password", "markus", new Properties)


		println(s"Markus accuracy: $rmse")
//		holdout.show
//		println(s"RMSE = ${rm.rootMeanSquaredError}")
//		println(s"R-squared = ${rm.r2}")
	}
}