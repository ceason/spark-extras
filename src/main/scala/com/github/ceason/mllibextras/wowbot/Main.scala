package com.github.ceason.mllibextras.wowbot

import com.github.ceason.mllibextras.LocalSpark

/**
  *
  */
object Main extends LocalSpark {

	def main(args: Array[String]): Unit = {

		val ws = new Defaults(spark)
			with Workspace
			with DefaultFeaturizer


		val x = 1
	}


}
