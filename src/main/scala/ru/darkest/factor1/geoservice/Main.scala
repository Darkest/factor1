package ru.darkest.factor1.geoservice

import java.nio.file.{Files, Paths}

import com.twitter.finagle.Http
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import ru.darkest.factor1.geoservice.services.Services

import scala.collection.JavaConverters._


case class ArgsConfig(appConf:Option[String] = None, usersToLoadFilepath:Option[String] = None, locsToLoadFilepath:Option[String] = None)

object Main extends App with LazyLogging {

  val parser = new scopt.OptionParser[ArgsConfig]("scopt") {
    opt[String]("ac").action( (x, c) =>
      c.copy(appConf = Some(x)) )

    opt[String]("users").action( (x, c) =>
      c.copy(usersToLoadFilepath = Some(x)) )

    opt[String]("locs").action( (x, c) =>
      c.copy(locsToLoadFilepath = Some(x)) )

  }

  val config = parser.parse(args, ArgsConfig()) match {
    case Some(conf) => conf
    case None =>
      logger.info("Arguments for starting App are broken. Proceeding with default config")
      ArgsConfig()
  }

  val appConfig: Config = config.appConf match {
    case Some(confPath) =>
      logger.info(s"Custom config file provided. Trying to load configuration from $confPath")
      val path = Paths.get(confPath)
      ConfigFactory.parseString(Files.readAllLines(path).asScala.mkString("\n"))
    case None =>
      ConfigFactory.load("application.conf")
  }
  val dbConfig = appConfig.getConfig("db")
  val db = new DB(dbConfig)
  db.createSchema()
  config.usersToLoadFilepath.foreach(db.loadUsersFromFile(_, block = 100000))
  config.locsToLoadFilepath.foreach(db.loadLocationsFromFile(_))

  val server = Http.server.serve(":9111", Services.locationService(db))

  println("Server is running")
  println(
    """            _______
      |Press q and |enter| to exit
      |            ^^^^^^^""".stripMargin)
  while(System.in.read()!= 'q') {

  }
}
