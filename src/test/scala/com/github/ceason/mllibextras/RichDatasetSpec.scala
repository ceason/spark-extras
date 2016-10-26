package com.github.ceason.mllibextras

import org.scalatest.FlatSpec
import com.github.ceason.mllibextras.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

import scala.util.Random

/**
  *
  */
class RichDatasetSpec extends FlatSpec
	with LocalSpark {

	import spark.implicits._



	"sliding" must "return things in proper order" in {

		val rows: List[r] = Random.shuffle(1 to 100).toList
			.zip(Random.shuffle(1 to 100).toList)
			.zip(Random.shuffle(1 to 100).toList)
		    .map{case ((a, b), c) ⇒ r(a, b, c)}

		val df: Dataset[r] = spark.createDataset(rows)

		val slid: Dataset[List[r]] = df.sliding(2, col("b"))

		val res = slid.map{
			case cur :: Nil ⇒
				true
			case cur :: next :: Nil ⇒
				cur.b == (next.b - 1)
		}.collect()

		assert(res.forall(_ == true))

	}

}
case class r(a: Int, b: Int, c: Int)