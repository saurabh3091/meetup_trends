package com.data.yaml

import net.jcazevedo.moultingyaml._
import org.apache.logging.log4j.scala.Logging

import scala.io.Source
import scala.util.control.NonFatal

case class PropConfigs(meetup: Map[String, String])

object YamlConfig extends DefaultYamlProtocol with Logging {
  lazy private val prop: PropConfigs = {
    try {
      val prop: PropConfigs =
        Source
          .fromFile("src/main/resources/configs/config.yaml")
          .mkString
          .parseYaml
          .convertTo[PropConfigs]

      logger.info(s"Config: $prop")
      prop
    }
    catch {
      case NonFatal(e) =>
        logger.error("Exception while creating YAML for configs: ", e)
        throw new IllegalAccessException()
    }
  }
  implicit val propConfigsFormat = yamlFormat1(PropConfigs)

  def apply(): PropConfigs = prop

}
