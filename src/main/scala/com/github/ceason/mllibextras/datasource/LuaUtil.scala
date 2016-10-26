package com.github.ceason.mllibextras.datasource

import org.luaj.vm2.{LuaTable, LuaValue}

/**
  *
  */
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
