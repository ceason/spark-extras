package com.github.ceason.mllibextras.wowbot

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import wowbot.gamedata.GameState


/**
  *
  */
object JdbcServer {


	def main(args: Array[String]): Unit = {

		val ss: SparkSession = SparkSession.builder
			.config("spark.sql.hive.thriftServer.singleSession", true)
			.enableHiveSupport()
			.master("local")
			.appName("spark session example")
			.getOrCreate()

		import ss.implicits._

		val sp = new Scratchpad {
			override val spark: SparkSession = ss
			override val maxGap: Int = 300
			override val gameStates: Dataset[GameState] = spark.createDataset(Seq.empty[GameState])
			override val actions: Dataset[ActionEvent] = spark.createDataset(Seq.empty[ActionEvent])
		}

		// TODO: reflectively register all of these things
		sp.actionWithGS
			.toDF("action", "gameState")
			.createOrReplaceTempView("actionWithGS")


		HiveThriftServer2.startWithContext(ss.sqlContext)


	}

}
