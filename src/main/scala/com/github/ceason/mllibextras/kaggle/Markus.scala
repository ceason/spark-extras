package com.github.ceason.mllibextras.kaggle
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, RegressionMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable
/**
  * Created by mbraasch on 10/27/16.
  */
object LogisticRegressionLoanDefaults {
	case class R(customerId: Int, limitBal: Int, male: Int, female: Int, education: Int, marriage: Int, age: Int,
		pay1: Int, pay2: Int, pay3: Int, pay4: Int, pay5: Int, pay6: Int,
		billAmt1: Int, billAmt2: Int, billAmt3: Int, billAmt4: Int, billAmt5: Int, billAmt6: Int,
		payAmt1: Int, payAmt2: Int, payAmt3: Int, payAmt4: Int, payAmt5: Int, payAmt6: Int, defaultOct: Int, defaultRaw: String)
	case class Params(
		input: String = null,
		testInput: String = "",
		dataFormat: String = "libsvm",
		regParam: Double = 0.0,
		elasticNetParam: Double = 0.0,
		maxIter: Int = 100,
		fitIntercept: Boolean = true,
		tol: Double = 1E-6,
		fracTest: Double = 0.2)
	lazy val spark = SparkSession
		.builder()
		.appName("Spark Loan Defaults")
		.master("local[4]")
		.config("spark.sql.warehouse.dir", "spark/spark-warehouse")
		.getOrCreate()
	def getInt(str: Any) = {
		str.toString match {
			case "NA" => -99999
			case x => x.toDouble.toInt
		}
	}
	def runJob() = {
		val data = spark.read.option("sep", ",").option("header", "true").csv("/Users/mbraasch/Downloads/data/train.csv")
		//implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
		import spark.implicits._
		/*data.take(3).map(x =>  R(x(0).toString.toInt, x(1).toString.toInt, if(x(2) == 1.0) 1 else 0, if(x(2) == 2.0) 1 else 0,
			x(3).toString.toDouble.toInt, x(4).toString.toDouble.toInt,
			x(5).toString.toDouble.toInt, x(6).toString.toDouble.toInt, x(7).toString.toDouble.toInt, x(8).toString.toDouble.toInt,
			x(9).toString.toDouble.toInt,
			x(10).toString.toDouble.toInt, x(11).toString.toDouble.toInt,
			x(12).toString.toDouble.toInt, x(13).toString.toInt, x(14).toString.toInt, x(15).toString.toInt,
			x(16).toString.toInt, x(17).toString.toInt, x(12).toString.toInt, x(13).toString.toInt,
			x(14).toString.toInt, x(15).toString.toInt, x(16).toString.toInt, x(17).toString.toInt,
			if (x(18) == "yes") 1 else 0
			))*/
		val rs = data.map(x =>  R(getInt(x(0)), getInt(x(1)), if(getInt(x(2)) == 1) 1 else 0, if(getInt(x(2)) == 2) 1 else 0,
			getInt(x(3)), getInt(x(4)), getInt(x(5)),
			getInt(x(6)), getInt(x(7)), getInt(x(8)), getInt(x(9)), getInt(x(10)), getInt(x(11)),
			getInt(x(12)), getInt(x(13)), getInt(x(14)), getInt(x(15)), getInt(x(16)), getInt(x(17)),
			getInt(x(18)), getInt(x(19)), getInt(x(20)), getInt(x(21)), getInt(x(22)), getInt(x(23)),
			if (x(24).toString == "yes") 1 else 0, x(24).toString
		))
		/*val splits = data.randomSplit(Array(0.8, 0.2), seed = 11L)
		val training = splits(0).cache()
		val test = splits(1)*/
		val categoricalVariables = Array("limitBal","male", "education", "marriage", "age", "pay1")
		val categoricalIndexers: Array[PipelineStage] =
			categoricalVariables.map( i => new StringIndexer()
				.setInputCol(i).setOutputCol(i+"Index").setHandleInvalid("skip") )
		val labelIndexer = new StringIndexer()
			.setInputCol("defaultOct")
			.setOutputCol("indexedLabel")
		val categoricalEncoders: Array[PipelineStage] =
			categoricalVariables.map(e => new OneHotEncoder()
				.setInputCol(e + "Index").setOutputCol(e + "Vec"))
		val assembler = new VectorAssembler()
			.setInputCols( Array(
				"limitBal", "male", "education", "marriage", "age","pay1"))
			//"limitBalVec", "maleVec", "educationVec", "marriageVec", "ageVec","pay1Vec"))
			.setOutputCol("features")
		val params = Params()
		// Run training algorithm to build the model
		val lr = new LogisticRegression()
			//.setLabelCol("indexedLabel")
			.setLabelCol("defaultOct")
			.setFeaturesCol("features")
			.setRegParam(params.regParam)
			.setElasticNetParam(params.elasticNetParam)
			.setMaxIter(params.maxIter)
			.setTol(params.tol)
			.setFitIntercept(params.fitIntercept)
		val steps = /*categoricalIndexers ++
categoricalEncoders ++
Array(labelIndexer) ++*/
			Array(assembler, lr)
		val pipeline = new Pipeline()
			.setStages(steps)
		// Fit the Pipeline.
		val startTime = System.nanoTime()
		/*val pipelineModel = pipeline.fit(training)
		/*val lr = new LinearRegression()
			.setLabelCol("price")
			.setFeaturesCol("features")
			.setMaxIter(1000)
			.setSolver("l-bfgs")*/
		// Compute raw scores on the test set.
		val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
			val prediction = lr.predict(features)
			(prediction, label)
		}
		// Get evaluation metrics.
		val metrics = new MulticlassMetrics(predictionAndLabels)
		val accuracy = metrics.accuracy
		println(s"Accuracy = $accuracy")
		/*data.map(x => {
			val defaultIndex = x.fieldIndex("default_oct")
			x(defaultIndex) = x.getAs[String]("default_oct")
		})*/*/
		/*val paramGrid = new ParamGridBuilder()
		.addGrid(lr.regParam, Array(0.1, 0.01, 0.001, 0.0001, 1.0))
		.addGrid(lr.fitIntercept)
		.addGrid(lr.elasticNetParam, Array(0.0, 1.0))
		.build()
		/val cv = new CrossValidator()
		.setEstimator(pipeline)
		//.setEvaluator( new RegressionEvaluator().setLabelCol("indexedLabel") )
		.setEvaluator( new RegressionEvaluator().setLabelCol("defaultOct") )
		.setEstimatorParamMaps(paramGrid)
		.setNumFolds(3)*/
		val Array(training, test) = rs.randomSplit(Array(0.8, 0.2), seed = 12345)
		val model = pipeline.fit(training)
		//val model = lr.fit(training) //cv.fit(training)
		//val holdout = model.transform(test).select("prediction","price")
		val predictions = model.transform(test)
		val evaluator = new RegressionEvaluator()
			.setMetricName("rmse")
			.setLabelCol("defaultOct")
			.setPredictionCol("prediction")
		val rmse = evaluator.evaluate(predictions)
		println(s"Root-mean-square error = $rmse")
		val holdout = predictions.select("prediction", "defaultOct").orderBy(abs(col("prediction")-col("defaultOct")))
		holdout.show
		val rm = new RegressionMetrics(holdout.rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))
		println(s"RMSE = ${rm.rootMeanSquaredError}")
		println(s"R-squared = ${rm.r2}")
	}
}