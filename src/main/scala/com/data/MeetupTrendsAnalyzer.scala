package com.data

import com.data.models._
import com.data.utils.DataUtil._
import com.data.utils.SparkUtil
import com.data.utils.SparkUtil._
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.ListMap

object MeetupTrendsAnalyzer extends Logging {
  def main(args: Array[String]): Unit = {
    //Initialize spark context
    implicit val spark: SparkSession = SparkUtil(appName = "Meetup-Trending-Topics")

    //parse input parameters
    val (jsonPath, topN : Int, perDay: String, start : Option[String], end: Option[String]) =
      args.length match {
          //process per day trends by default
        case 2 => (args(0), args(1).toInt, "Y", None, None)
        case 3 => (args(0), args(1).toInt, args(2), None, None)
        case 4 => (args(0), args(1).toInt, args(2), Some(args(3)), None)
        case 5 => (args(0), args(1).toInt, args(2), Some(args(3)), Some(args(4)))
        case _ => throw new IllegalArgumentException(
          """
            |Please specify json file path, N for top N records, Y for per day/N for over total time period(Default Y)
            |and optionally start and end dates all separated by space to get trending topics
            |Examples:
            |src/main/resources/meetup.json 10
            |src/main/resources/meetup.json 10 N
            |src/main/resources/meetup.json 20 Y 2018-03-10
            |src/main/resources/meetup.json 30 N 2017-03-10 2018-04-10
          """.stripMargin)
      }

    process(jsonPath, topN, parseInputToBoolean(perDay), start, end)
    spark.stop()
  }

  def process(path: String, n: Int, perDay: Boolean, start: Option[String], end: Option[String])(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val schema: StructType = Encoders.product[RSVP].schema

    val meetupDataset: Dataset[RSVP] =
      readJSONAsDataFrame(path, schema = schema).as[RSVP]

    //filter events here based on input time
    val timeFilteredEvents: Dataset[RSVP] =
      getTimeFilteredEvents(meetupDataset, start, end, perDay)

    //persist for faster access from memory
    timeFilteredEvents.persist(StorageLevel.MEMORY_AND_DISK_SER)

    if(perDay)
      generateMeetupTrendsByDay(timeFilteredEvents,n)
    else
      generateMeetupTrends(timeFilteredEvents, n)
  }

  /**
    * Find trending topics per day from meetup.json and output them to a json file
    * @param meetupEventsDS RSVP events dataset
    * @param n Top N trending words
    * @param spark implicit SparkSession
    */
  def generateMeetupTrendsByDay(meetupEventsDS: Dataset[RSVP], n: Int)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val dateWiseKeywords: Dataset[(String, List[String])] = meetupEventsDS
      .flatMap{ rsvp =>
        val keywords = getRSVPKeywords(rsvp)
        if (keywords.isDefined)
          Some(getRoundedOffDateFromEpoch(rsvp.mtime.get), keywords.get)
        else
          None
      }

    val groupedDatewise: Dataset[(String, List[String])] =
      dateWiseKeywords.groupByKey(_._1).reduceGroups((a, b) => (a._1, a._2 ++ b._2)).map(a => (a._1, a._2._2))

    val finalOutput = groupedDatewise.map{
      case (date: String, keywords: List[String])=>
        Map[String, Map[String, Int]](date ->
          ListMap(
            keywords
              .map(w => (w, 1))
              .groupBy(_._1)
              .map{ case (keyword: String, keywords: List[(String, Int)]) => (keyword, keywords.length)}.toSeq
              .sortBy(_._2)(Ordering[Int].reverse)
              .take(n):_*)
        )
    }

    saveDatasetAsJSON(finalOutput, "trending_topics_per_day.json")
  }

  /**
    * Find overall trending topics from meetup.json and output them to a json file
    * @param meetupEventsDS RSVP events dataset
    * @param n Top N trending words
    * @param spark implicit SparkSession
    */
  def generateMeetupTrends(meetupEventsDS: Dataset[RSVP], n: Int)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val words = meetupEventsDS
      .flatMap(getRSVPKeywords).flatMap(_.map(_.toString))

    val wordCount = words.groupBy("value").count()
    //using ListMap to retain list order while converting to Map
    val trends = ListMap(wordCount.sort(desc("count")).take(n).map(a => (a.get(0), a.get(1))).toList:_*)

    writeMapToFile(trends, path = "trending_topics.json")
  }

  /**
    * Filter events based on a timeframe specified at input
    * @param ds RSVP events dataset
    * @param start start date YYYY-MM-dd
    * @param end End date YYYY-MM-dd
    * @param isDayWise are the trending topics required per day or overall
    * @return [[Dataset[RSVP]]
    */
  def getTimeFilteredEvents(ds: Dataset[RSVP], start: Option[String], end: Option[String], isDayWise: Boolean): Dataset[RSVP] = {
    (start, end) match {
      //only start date is specified
      case (Some(s), None) =>
        val epochStart = getEpochFromDateString(s)
        logger.info(s"Filtering events older than :$s")
        ds.filter(event => event.mtime.isDefined && event.mtime.get > epochStart)

      //both start and end dates are supplied to get trending topics
      case (Some(s), Some(e)) =>
        val epochStart = getEpochFromDateString(s)
        val epochEnd = getEpochFromDateString(e)
        logger.info(s"Filtering events older than :$s and later than : $e")
        ds.filter(event => event.mtime.isDefined && event.mtime.get > epochStart && event.mtime.get <= epochEnd)

      //consider complete dataset in case no date is supplied
      case _ =>
        //if daywise output is required then mtime is mandatory
        if (isDayWise)
          ds.filter(event => event.mtime.isDefined)
        else
          ds
    }
  }

  /**
    * Generate keywords from meetup title and groups to find trending topics
    * @param rsvpEvent [[RSVP]] event
    * @return Optional list of keywords extracted from RSVP events
    */
  def getRSVPKeywords(rsvpEvent: RSVP): Option[List[String]] = {
    val keywords = (rsvpEvent.event, rsvpEvent.group) match{
      case (Some(event: Event), Some(group: Group)) =>
        val titleKeys = event.event_name.map(_.split(" ").toList)
        val groupKeys = group.group_topics
          .map(topic => topic.flatMap(_.topic_name).flatMap(_.split(" ")))

        Option(List(titleKeys, groupKeys).flatten.flatten)

      case (Some(event: Event), None) =>
        event.event_name.map(_.split(" ").toList)

      case (None, Some(group: Group)) =>
        group.group_topics.map(topic => topic.flatMap(_.topic_name).flatMap(_.split(" ")))

      case _ => None
    }

    //filter words like on,for,to,and etc
    keywords.map(_.filter(_.length > 3).map(_.trim.toLowerCase))
  }
}