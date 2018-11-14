package com.data.utils

import com.data.utils.DataUtil._
import org.scalatest.{FlatSpec, Matchers}

class DataUtilSpec extends FlatSpec with Matchers {
  "getEpochFromDateString" should "return epoch from date of format YYYY-MM-DD" in {

    getEpochFromDateString(dateString = "2018-06-04") should be(1528063200000L)
  }

  "getRoundedOffDateFromEpoch" should "returns date of day start from any given epoch" in {

    getRoundedOffDateFromEpoch(epoch = 1541867426000L) should be("2018-11-10")

  }

  "parseInputToBoolean" should "returns Boolean corresponding to input parameter supplied" in {

    parseInputToBoolean(input = "Y") should be(true)
    parseInputToBoolean(input = "y") should be(true)
    parseInputToBoolean(input = "1") should be(true)
    parseInputToBoolean(input = "N") should be(false)
    parseInputToBoolean(input = "n") should be(false)
    parseInputToBoolean(input = "0") should be(false)
    parseInputToBoolean(input = "abc") should be(false)

  }
}
