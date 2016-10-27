package com.github.ceason.mllibextras.wowbot

import java.sql.Timestamp

import wowbot.gamedata.GameState
import org.apache.spark.ml.linalg.{Vector ⇒ MLVector}

/**
  *
  */
case class ActionEvent(
	timestamp: Timestamp,
	spellId: Long
)

case class FeaturizedData(
	actionEvent: ActionEvent,
	spellId: Long,
	gameState: GameState,
	features: MLVector
)

case class Feature(name: String, value: Double)