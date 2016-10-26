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







