package com.data.models
/*
These classes manage the Models required for processing Meetup RSVP events
 */

case class RSVP(event: Option[Event], group: Option[Group], mtime: Option[Long])

case class Event(event_name: Option[String], time: Option[Long])

case class Group(group_city: String, group_country: String, group_topics: Option[List[Topic]])

case class Topic(urlkey: String, topic_name: Option[String])