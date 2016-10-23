package com.github.ceason.mllibextras.datasource

import org.luaj.vm2.LuaValue
import org.luaj.vm2.lib.jse.JsePlatform
import org.scalatest.FlatSpec

/**
  *
  */
class LuaUtilSpec extends FlatSpec {

	val luaValue: LuaValue = {
		val global = JsePlatform.standardGlobals()
		global.get("dofile").call( LuaValue.valueOf("TrainingDummyOutput_20161016-1.lua") ) // opens file, runs contents
		global.get("WTFTestAddonData")
	}

	behavior of "LuaUtilSpec"

	it should "luaTable2Map" in {



	}

	it should "lua2scala" in {

		val res = LuaUtil.lua2scala(luaValue)

		assert(true)
	}

}
