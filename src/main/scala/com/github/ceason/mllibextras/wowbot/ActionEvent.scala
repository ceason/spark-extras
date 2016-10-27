package com.github.ceason.mllibextras.wowbot

import java.sql.Timestamp

import wowbot.gamedata.GameState

/**
  *
  */
case class ActionEvent(
	timestamp: Timestamp,
	spellId: Long
)

case class ProjectData(
	actionEvent: ActionEvent,
	spellId: Long,
	gameState: GameState
)

case class Feature(name: String, value: Double)