package com.github.ceason.mllibextras.wowbot

import com.github.ceason.mllibextras.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import wowbot.gamedata.GameState

/**
  *
  */
trait Scratchpad extends Actions
	with GameStates {

	val spark: SparkSession

	val maxGap: Int

	import spark.implicits._


	lazy val bucketedGStates: KeyValueGroupedDataset[Long, GameState] = {
		gameStates.groupByKey(_.combatLogTimestamp / 1000)
	}

	lazy val bucketedActions: KeyValueGroupedDataset[Long, ActionEvent] = {
		actions.groupByKey(_.timestamp.getTime / 1000)
	}

	lazy val actionWithGS: Dataset[(ActionEvent, GameState)] = {
		bucketedActions.cogroup(bucketedGStates){
			case (bucket, as, gs) ⇒

				// sort gstates newest to oldest
				val sortedGs = gs.toList.sortBy(-1 * _.combatLogTimestamp)

				// find the "first" gs that preceeded action (aka most recently preceeding)
				as.flatMap{ a ⇒
					sortedGs
						.find(_.combatLogTimestamp < a.timestamp.getTime)
						.map(a → _)
						.toList
				}
		}
	}



}
