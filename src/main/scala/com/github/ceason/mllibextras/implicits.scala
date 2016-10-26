package com.github.ceason.mllibextras

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, Dataset, Encoder}
import org.apache.spark.sql.functions._

/**
  *
  */
object implicits {


	implicit class RichDataset[A](val ds: Dataset[A]) extends AnyVal {

		import ds.sparkSession.implicits._


		def sliding(n: Int, orderCol: Column)(implicit ev: Encoder[List[A]], ev2: Encoder[Array[A]]): Dataset[List[A]] = {

			ds.select(
				collect_list(struct("*")) over Window
					.orderBy(orderCol)
					.rowsBetween(0, n - 1)
			).as[Array[A]].map(_.toList)
		}
	}

}
