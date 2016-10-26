package com.github.ceason.mllibextras.wowbot

import org.apache.spark.sql.Dataset
import wowbot.gamedata.GameState

/**
  *
  */
trait GameStates {

	val gameStates: Dataset[GameState]

}
