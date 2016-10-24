package com.github.ceason.mllibextras.datasource

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.luaj.vm2.lib.jse.JsePlatform
import org.luaj.vm2.{LuaTable, LuaValue}

import scala.collection.immutable.Iterable


/**
  *
  */
class DefaultSource extends RelationProvider {

	override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
		parameters.get("path") match {
			case Some(path) ⇒ new SavedVariablesRelation(sqlContext, path)
			case None ⇒ throw new IllegalArgumentException("Path is required")
		}
	}
}

case class SVDamage(spellId: Long, amount: Long)
case class SVAction(spellId: Long)
case class SVRow(
	timestamp: Timestamp,
	action: Option[SVAction] = None,
	damage: Option[SVDamage] = None
)

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

protected[datasource] object LuaUtil {


	def lua2scala(value: LuaValue): Any = {
		value.`type` match {
			case LuaValue.TSTRING ⇒ value.checkjstring
			case LuaValue.TNUMBER ⇒
				val num = value.checkdouble
				val isWholeNumber = num == num.floor
				if (isWholeNumber)
					value.checklong
				else num

			case LuaValue.TINT    ⇒ value.checklong
			case LuaValue.TTABLE  ⇒ luaTable2Map(value.checktable).map{
				case (k, v) ⇒ lua2scala(k) → lua2scala(v)
			}
		}
	}


	def luaTable2Map(t: LuaTable): Map[LuaValue, LuaValue] = {
		// TODO: recursively decompose the input table into pattern matchable stuff!

		def loop(key: LuaValue, res: Map[LuaValue, LuaValue]): Map[LuaValue, LuaValue] = {
			val n = t.next(key)
			if (n.arg1().isnil()) res
			else {
				val key = n.arg(1)
				val value = n.arg(2)
				loop(key, res + (key → value))
			}
		}

		loop(LuaValue.NIL, Map.empty)
	}
}