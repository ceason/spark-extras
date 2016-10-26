package com.github.ceason.mllibextras.wowbot

import java.sql.Timestamp

/**
  *
  */
case class ActionEvent(
	timestamp: Timestamp,
	spellId: Long
)
