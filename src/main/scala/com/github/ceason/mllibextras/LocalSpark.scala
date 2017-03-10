package com.github.ceason.mllibextras

import org.apache.spark.sql.SparkSession

/**
  *
  */
trait LocalSpark {

	val spark: SparkSession = SparkSession.builder
		.master("local[*]")
		.appName("spark session example")
		.getOrCreate()

}
