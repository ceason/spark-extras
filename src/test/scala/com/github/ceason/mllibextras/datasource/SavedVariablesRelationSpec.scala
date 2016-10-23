package com.github.ceason.mllibextras.datasource

import com.github.ceason.mllibextras.LocalSpark
import org.scalatest.FlatSpec

/**
  *
  */
class SavedVariablesRelationSpec extends FlatSpec
	with LocalSpark {


	"SavedVariablesRelation" must "load test dataset without error" in {

		val df = spark.read
			.format("com.github.ceason.mllibextras.datasource")
		    .load("TrainingDummyOutput_20161016-1.lua")

		assert(true)
	}
}
