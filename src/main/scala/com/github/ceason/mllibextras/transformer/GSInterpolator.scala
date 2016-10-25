package com.github.ceason.mllibextras.transformer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  *
  */
class GSInterpolator(val uid: String)
	extends Transformer with DefaultParamsWritable {

	/*
		what is this thing doing??

			- fill in spaces between specified bucket column?
			- output column is offset from latest real column?
			- param for maximum gap size to fill in?

	 */

	val inputCol : Param[String] = new Param(this, "inputCol", "input column name")
	val outputCol: Param[String] = new Param(this, "outputCol", "column name to put offsets")
	val maxGap   : Param[Int]    = new Param(this, "maxGap", "maximum gap to interpolate")


	def setInputCol(value: String): this.type = set(inputCol, value)
	def setOutputCol(value: String): this.type = set(outputCol, value)
	def setMaxGap(value: Int): this.type = set(maxGap, value)

	// Empty constructor for reflective runtime instantiation from R/Python
	def this() = this(Identifiable.randomUID("gsinterpolator"))

	override def transformSchema(schema: StructType): StructType = {

		val validInputTypes: Set[DataType] = Set(LongType, IntegerType, ShortType)

		val inputType = schema($(inputCol)).dataType

		val outColName = $(outputCol)

		require(!schema.fieldNames.contains(outColName), "outColName already exists!")
		require(validInputTypes contains inputType, s"invalid input type $inputType; valid types are ${validInputTypes.mkString(",")}")


		val outCol = StructField(outColName, LongType)

		StructType(schema.fields :+ outCol)
	}


	override def transform(dataset: Dataset[_]): DataFrame = {



		/*
			steps...

				- sort by input col ascending
				- zip with index
					- structure as "index, struct(*)"
				- left join on self where index = index + 1
					- structure: "current, next"
				- flatMap
					- return `0 -> current` if next is null
					- return `0 -> current` if (next - current) > max gap
					- return `0 -> current` if (next - current) == 0
					- return (0 until (next - current)).map(offset -> current)

		 */

		val withRowNum = dataset.select(
			struct("*") as 'data,
			row_number() over Window.orderBy($(inputCol)) as 'rowNum
		)

		val withNext = withRowNum.as('current).join(withRowNum.as('next),
			joinExprs = expr("current.rowNum = next.rowNum - 1"),
			joinType = "left_outer"
		).select(
			col("current.data") as 'current,
			col("next.data") as 'next,
			expr(s"next.data.${$(inputCol)} - current.data.${$(inputCol)}") as 'delta
		)









		???
	}

	/**
	  * Creates a copy of this instance with the same UID and some extra params.
	  * Subclasses should implement this method and set the return type properly.
	  * See `defaultCopy()`.
	  */
	override def copy(extra: ParamMap): Transformer = {
		val that = new GSInterpolator(uid)
		copyValues(that, extra)
	}



}
