package com.github.ceason.mllibextras.wowbot

import java.sql.Timestamp

import com.github.ceason.mllibextras.datasource.SVAction
import org.apache.spark.sql.Dataset

/**
  *
  */
trait Actions {

	val actions: Dataset[ActionEvent]
}

case class ActionEvent(
	timestamp: Timestamp,
	spellId: Long
)