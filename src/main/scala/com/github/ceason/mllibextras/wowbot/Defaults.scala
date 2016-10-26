package com.github.ceason.mllibextras.wowbot

import com.github.ceason.mllibextras.datasource.SVRow
import org.apache.spark.sql.{Dataset, SparkSession}
import wowbot.gamedata.GameState

/**
  *
  */
class Defaults(val spark: SparkSession) {


	import spark.implicits._

	val maxGap = 300

	val actions: Dataset[ActionEvent] = spark.read
		.format("com.github.ceason.mllibextras.datasource")
		.load("C:\\Inst\\zeppelin-0.6.2-bin-all\\bin\\TrainingDummyOutput_20161016-1.lua")
	    .as[SVRow]
	    .flatMap { r ⇒
			r.action.map{ a ⇒
				ActionEvent(r.timestamp, a.spellId)
			}.toList
		}.cache()

	val gameStates: Dataset[GameState] = {
		spark.read
			.parquet("C:\\Inst\\zeppelin-0.6.2-bin-all\\bin\\TrainingDummyOutput_20161016-1.parquet")
		    .as[GameState]
	}.cache()


}
