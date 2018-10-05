package ru.darkest.factor1.geoservice.config

import com.typesafe.config.Config

import scala.util.Try

object ConfigExtensions {

  implicit class UberConfig(val c: Config) extends AnyVal {
    def getStringOrFail(parName: String) = {
      Try(c.getString(parName)).toOption.getOrElse(throw new Exception(s"No $parName specified in the config"))
    }
  }

}
