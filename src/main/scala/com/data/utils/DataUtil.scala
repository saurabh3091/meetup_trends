package com.data.utils

import java.io.{File, PrintWriter}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object DataUtil {
  private  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
    * This function returns epoch for date string in format YYYY-MM-dd
    * @param dateString date as [[String]] in format YYYY-MM-dd
    * @return Epoch as [[Long]]
    */
  def getEpochFromDateString(dateString : String): Long = {
    try{
      DateTimeFormat.forPattern("YYYY-MM-dd").parseDateTime(dateString).getMillis
    }
    catch {
      case e: Exception => throw new IllegalArgumentException(
        """Please provide valid dates in format YYYY-MM-dd:
          | Ex: 2018-09-10
        """.stripMargin)
    }
  }

  /**
    * Rounds off epoch to beginning of day
    * @param epoch epoch as [[Long]]
    * @return [[Long]] rouded off date to start of day in format YYYY-MM-dd
    */
  def getRoundedOffDateFromEpoch(epoch: Long): String = {
    new DateTime(epoch).withTimeAtStartOfDay().toString("YYYY-MM-dd")
  }

  /**
    * Returns Boolean equivalent of input params
    * @param input user input as Y/y/0/1
    * @return [[Boolean]] equivalent
    */
  def parseInputToBoolean(input:String): Boolean = {
    input.trim.toLowerCase match {
      case "y" | "1" => true
      case _ => false
    }
  }

  /**
    * Write [[Map]] as Json to file
    * @param map input [[Map]] to write as JSON to file
    * @param path [[File]] path where it will be written
    */
  def writeMapToFile(map: Map[Any, Any], path: String): Unit = {

    val writer = new PrintWriter(new File(path))

    writer.write(mapper.writeValueAsString(map))
    writer.close()
  }
}
