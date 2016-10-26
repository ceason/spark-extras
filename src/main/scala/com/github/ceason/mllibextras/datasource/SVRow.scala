package com.github.ceason.mllibextras.datasource

import java.sql.Timestamp

/**
  *
  */
case class SVRow(
	timestamp: Timestamp,
	action: Option[SVAction] = None,
	damage: Option[SVDamage] = None
)
case class SVAction(spellId: Long)
case class SVDamage(spellId: Long, amount: Long)
