package com.data.utils

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

object SparkUtil extends Logging {
  /**
    * Creates and returns a spark session
    *
    * @param appName Name of your spark application
    * @return Returns a [[SparkSession]]
    */
  def apply(appName: String): SparkSession = {
    val conf = new SparkConf()
    if (conf.getOption("spark.master").isEmpty)
      conf.setMaster("local[*]")

    val spark: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .getOrCreate()

    spark
  }

  /**
    * Reads a JSON as [[DataFrame]]
    *
    * @param path   Path of the JSON to read
    * @param schema [[StructType]] to read json as strongly typed DataSet(avoids issues for casting datatypes later)
    * @param spark  implicit [[SparkSession]]
    * @return a [[DataFrame]]
    */
  def readJSONAsDataFrame(path: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    logger.info("Reading JSON from path " + path)
    spark
      .read
      .schema(schema)
      .json(path)
  }

  /**
    * Saves a [[Dataset]] as JSON file having a single partition
    *
    * @param ds   Inpt dataset to be saved
    * @param path Path for the JSON to be saved
    */
  def saveDatasetAsJSON(ds: Dataset[Map[String, Map[String, Int]]], path: String): Unit = {
    ds
      .coalesce(numPartitions = 1)
      .write.mode(SaveMode.Overwrite)
      .json(path)

    logger.info("Output successfully saved to " + path)
  }
}
