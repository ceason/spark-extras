package com.github.ceason.mllibextras.datasource

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.luaj.vm2.LuaValue
import org.luaj.vm2.lib.jse.JsePlatform

/**
  *
  */
class SavedVariablesRelation(
	@transient val sqlContext: SQLContext,
	path: String
) extends BaseRelation
	with TableScan
	with Serializable {




	val schema: StructType = {
		sqlContext
			.createDataFrame(Seq.empty[SVRow])
			.schema
	}

	/*
	val schema: StructType = StructType(
		StructField("timestamp", TimestampType, nullable = false) ::
		StructField("action", StructType(
			StructField("spellId", IntegerType, nullable = false) ::
			Nil
		)) ::
		StructField("damage", StructType(
			StructField("spellId", IntegerType, nullable = false) ::
			StructField("amount" , LongType   , nullable = false) ::
			Nil
		)) ::
		Nil
	)
	// */

	def buildScan(): RDD[Row] = {
		/*
		// this structure must match the schema's structure
		val rows: Seq[(Long, Option[Tuple1[Long]], Option[(Long, Long)])] = luaData.flatMap {
			case ("actionEvents", v: Map[Long, Map[Long, Long]]) ⇒
				for {
					(spellId, occurrences) ← v.toSeq
					tstamp ← occurrences.values
				} yield (tstamp, Some(Tuple1(spellId)), None)

			case ("damageEvents", v: Map[Long, Map[Long, Long]]) ⇒
				for {
					(spellId, occurrences) ← v.toSeq
					(tstamp, amount) ← occurrences
				} yield (tstamp, None, Some(spellId → amount))
		}.toSeq
		*/
		sqlContext
			.createDataFrame(rows)
			.rdd
	}

	private def rows: Seq[SVRow] = luaData.flatMap {
		case ("actionEvents", v: Map[Long, Map[Long, Long]]) ⇒
			for {
				(spellId, occurrences) ← v.toSeq
				tstamp ← occurrences.values
			} yield SVRow(
				timestamp = new Timestamp(tstamp),
				action = Some(SVAction(spellId))
			)

		case ("damageEvents", v: Map[Long, Map[Long, Long]]) ⇒
			for {
				(spellId, occurrences) ← v.toSeq
				(tstamp, amount) ← occurrences
			} yield SVRow(
				timestamp = new Timestamp(tstamp),
				damage = Some(SVDamage(spellId, amount))
			)
	}.toSeq


	private def luaData: Map[String, Any] = {
		val global = JsePlatform.standardGlobals()

		global.get("dofile").call( LuaValue.valueOf(path) ) // opens file, runs contents

		val lValue = global.get("WTFTestAddonData")
		LuaUtil.lua2scala(lValue) match {
			case m: Map[String, Any] ⇒ m
		}
	}

}
