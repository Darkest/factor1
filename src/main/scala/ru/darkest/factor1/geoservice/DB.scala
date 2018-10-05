package ru.darkest.factor1.geoservice

import java.sql.{Connection, Statement}

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2.BasicDataSource
import ru.darkest.factor1.geoservice.Schema.{LocationTile, User}
import ru.darkest.factor1.geoservice.config.ConfigExtensions._
import ru.darkest.factor1.geoservice.services.dao.{Location, UserWithLocation}
import ru.darkest.factor1.geoservice.utils.Utils

import scala.util.{Failure, Success, Try}

object Schema {
  val UsersTableDefinition: String =
    """
      |CREATE TABLE IF NOT EXISTS USERS(
      |ID BIGINT,
      |LAT DOUBLE,
      |LON DOUBLE,
      |PRIMARY KEY(ID)
      |)
    """.stripMargin

  val UserLatIndex: String =
    """
      |CREATE INDEX USERS_LAT_INDEX
      |ON USERS (LAT)
    """.stripMargin

  val UserLonIndex: String =
    """
      |CREATE INDEX USERS_LON_INDEX
      |ON USERS (LON)
    """.stripMargin

  val LocationTilesDefinition: String =
    """
      |CREATE TABLE IF NOT EXISTS TILES(
      |LAT INT,
      |LON INT,
      |E INT,
      |PRIMARY KEY(LAT, LON),
      |CHECK(E > 0)
      |)
    """.stripMargin

  case class LocationTile(lat: Int, lon: Int, e: Int)

  case class User(id: Long, lat: Double, lon: Double)

}

class DB(config: Config) extends LazyLogging {
  private val ds: BasicDataSource = new BasicDataSource
  setConfiguration(config)

  def createSchema(): Boolean = {
    logger.debug(s"Creating Users and Locations tables schemes")
    getConnection.map { conn =>
      Utils.withResources(conn) { c =>
        implicit val conn: Connection = c
        val createUsers = getStatement.map(_.execute(Schema.UsersTableDefinition))
        val latIndex = getStatement.map(_.execute(Schema.UserLatIndex))
        val lonIndex = getStatement.map(_.execute(Schema.UserLonIndex))
        val createLocationtTiles = getStatement.map(_.execute(Schema.LocationTilesDefinition))

        val result = (createUsers ::
          createLocationtTiles ::
          latIndex ::
          lonIndex ::
          Nil).map {
          case x@Success(_) => x
          case x@Failure(exception) =>
            logger.error(exception.toString)
            x
        }.forall {
          case Success(_) => true
          case Failure(_) => false
        }
        if (result)
          logger.debug(s"Tables Users and Locations successfully created")
        else
          logger.error(s"Some DB objects were not created!!! Check for ERRORS above!")

        result
      }
    }.getOrElse(false)
  }

  def updateUserLocation(userWithLocation: UserWithLocation): Try[Int] = {
    import userWithLocation._
    getConnection.flatMap { conn =>
      Utils.withResources(conn) { c =>
        implicit val connection: Connection = c
        val updateSql = s"MERGE INTO USERS KEY(ID) VALUES ($id, ${loc.lat}, ${loc.lon})"
        getStatement.map { statement =>
          logger.info(s"Executing '$updateSql'")
          val updateCount = statement.executeUpdate(updateSql)
          statement.close()
          updateCount
        }
          .recoverWith { case exc =>
            logger.error(s"Updating user location failed ('$updateSql')", exc)
            Failure(exc)
          }
      }
    }
  }

  def getEforTile(lat: Double, lon: Double): Try[Int] = {
    getEforTile(lat.toInt, lon.toInt)
  }

  def getEforTile(lat: Int, lon: Int): Try[Int] = {
    getConnection.flatMap { conn =>
      Utils.withResources(conn) { c =>
        implicit val connection: Connection = c
        val selectSql = s"SELECT E FROM TILES WHERE LAT=$lat AND LON=$lon"
        getStatement.map(_.executeQuery(selectSql))
          .map { resultSet =>
            resultSet.next()
            resultSet.getInt("E")
          }
          .recoverWith { case exc =>
            logger.info("", exc)
            logger.error(s"Getting error for tile failed ('$selectSql')")
            Failure(exc)
          }
      }
    }
  }

  def loadUsersFromFile(filepath: String, block: Int = 100000): (Int, Int) = {
    readAndLoad[User](filepath, block)(stringToUser())(loadUsers)
  }

  private def loadUsers(users: Seq[User]): Int = {
    getConnection.flatMap { conn =>
      Utils.withResources(conn) { c =>
        implicit val connection: Connection = c
        getStatement.map { statement =>
          logger.debug(s"Adding ${users.size} users to DB")
          users.foreach { user =>
            val insert = s"INSERT INTO USERS (id, lat, lon) values ('${user.id}', '${user.lat}', '${user.lon}')"
            statement.addBatch(insert)
            logger.debug(s"'$insert' added to batch")
          }
          val updatesCount = statement.executeBatch().sum

          connection.commit()
          updatesCount
        }
      }
    }.recover { case exc => logger.error("", exc); 0 }.getOrElse(0)
  }

  private def stringToUser(separator: String = " ")(string: String): Option[User] = {
    Try {
      string.split(separator).toList match {
        case id :: lat :: lon :: Nil => User(id.toLong, lat.toDouble, lon.toDouble)
        case str => throw new Exception(s"String($str) is not the right format for User(id: Long, lat: Double, lon: Double)")
      }
    }.recoverWith{case exc =>
      logger.error(s"Could not parse '$string' to User(id: Long, lat: Double, lon: Double) due to:", exc)
      Failure(exc)
    }.toOption
  }

  def loadLocationsFromFile(filepath: String, block: Int = 100000): (Int, Int) = {
    readAndLoad[LocationTile](filepath, block)(stringToLocationTile())(loadLocations)
  }

  private def loadLocations(locations: Seq[LocationTile]): Int = {
    getConnection.flatMap { conn =>
      implicit val connection: Connection = conn
      getStatement.map { statement =>
        logger.debug(s"Adding ${locations.size} location tiles to DB")
        locations.foreach { location =>
          val insert = s"Insert into TILES(LAT, LON, E) values ('${location.lat}', '${location.lon}', ${location.e})"
          logger.trace(insert)
          statement.addBatch(insert)
        }
        val updatesCount = statement.executeBatch().sum
        statement.close()
        updatesCount
      }
    }.recover { case exc => logger.error("", exc); 0 }.getOrElse(0)
  }

  private def stringToLocationTile(separator: String = " ")(string: String): Option[LocationTile] = {
    Try {
      string.split(separator).toList match {
        case lat :: lon :: e :: Nil => LocationTile(lat.toInt, lon.toInt, e.toInt)
        case str => throw new Exception(s"String($str) is not the right format for LocationTile(lat: Int, lon: Int, e: Double)")
      }
    }.recoverWith { case exc =>
      logger.error(s"Could not parse '$string' to case class LocationTile(lat: Int, lon: Int, e: Double) due to exception:", exc)
      Failure(exc)
    }.toOption
  }

  private def readAndLoad[T](filepath: String, block: Int = 10000)
                            (stringToEntity: String => Option[T])
                            (entitiesToDb: Seq[T] => Int): (Int, Int) = {
    var recordsProcessed = 0
    var recordsLoaded = 0
    Try {
      val buffer = new scala.collection.mutable.ListBuffer[T]
      Utils.withResources(scala.io.Source.fromFile(filepath)) { source =>
        val linesIterator = source.getLines()
        while (linesIterator.hasNext) {
          stringToEntity(linesIterator.next()) match {
            case Some(entity) =>
              buffer += entity
              if (buffer.size == block) {
                recordsLoaded += entitiesToDb(buffer)
                buffer.clear()
              }
            case None =>
          }
          recordsProcessed += 1
        }

      }
      recordsLoaded += entitiesToDb(buffer)
    }.recover { case exc => logger.error("", exc) }

    if (recordsLoaded == recordsProcessed)
      logger.info(s"All records($recordsLoaded) successfully added to DB ")
    else
      logger.info(s"$recordsProcessed Records read from $filepath, only $recordsLoaded records loaded to DB" +
        s"Look for errors in log above!")
    (recordsProcessed, recordsLoaded)
  }

  def getUser(id: Long): Try[User] = {
    getConnection.flatMap { conn =>
      Utils.withResources(conn) { c =>
        implicit val connection: Connection = c
        getStatement.map { statement =>
          val select = s"SELECT LAT, LON FROM USERS WHERE ID = $id"
          val result = statement.executeQuery(select)
          result.next()
          val lat = result.getDouble("LAT")
          val lon = result.getDouble("LON")
          User(id, lat, lon)
        }.recover { case exc => logger.error("", exc)
          throw exc
        }
      }
    }
  }

  def getUsersInSquare(lat: (Double, Double), lon: (Double, Double)): Try[List[UserWithLocation]] = {
    getConnection.flatMap { conn =>
      Utils.withResources(conn) { c =>
        implicit val connection: Connection = c
        getStatement.map { statement =>
          val (latMin, latMax) = lat
          val (lonMin, lonMax) = lon
          val select = s"SELECT * FROM USERS WHERE LAT>=$latMin AND LAT<=$latMax AND LON>=$lonMin AND LON<=$lonMax"
          val resultSet = statement.executeQuery(select)
          val result = scala.collection.mutable.ListBuffer[UserWithLocation]()
          while (resultSet.next()) {
            val userId = resultSet.getLong("ID")
            val lat = resultSet.getDouble("LAT")
            val lon = resultSet.getDouble("LON")
            result += UserWithLocation(userId, Location(lat, lon))
          }
          result.toList
        }
      }
    }
  }

  private def getStatement(implicit conn: Connection): Try[Statement] = {
    Try {
      conn.createStatement
    }
  }

  private def getConnection: Try[Connection] = {
    Try {
      ds.getConnection()
    }
  }

  private def setConfiguration(dbConfig: Config) = {
    val driver = dbConfig.getStringOrFail("driver")
    val url = dbConfig.getStringOrFail("url")
    val user = dbConfig.getStringOrFail("user")
    val pass = dbConfig.getStringOrFail("password")

    logger.debug(s"Setting up DataSource for $url with driver($driver) and user($user)/password($pass)")
    ds.setDriverClassName(driver)
    ds.setUrl(url)
    ds.setUsername(user)
    ds.setPassword(pass)
    ds.setMaxWaitMillis(500)
  }
}