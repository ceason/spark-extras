package com.github.ceason.mllibextras

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Dataset

import scala.collection.JavaConverters._

/**
  *
  */
class ParallelPipeline extends Pipeline {

	override def fit(dataset: Dataset[_], paramMaps: Array[ParamMap]): Seq[PipelineModel] = {
		paramMaps.par.map(fit(dataset, _)).seq
	}
}
