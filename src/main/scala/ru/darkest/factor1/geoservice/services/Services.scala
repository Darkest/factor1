package ru.darkest.factor1.geoservice.services

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.io.Buf
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.generic.auto._
import io.finch
import io.finch._
import io.finch.circe._
import io.finch.syntax._
import ru.darkest.factor1.geoservice.DB
import ru.darkest.factor1.geoservice.geoCalculus.GeoCalculus
import ru.darkest.factor1.geoservice.services.dao.{InLocation, Location, UserWithLocation}

import scala.util.{Success, Try}

object Services extends LazyLogging {

  def userInLocationEndpoint(db: DB): Endpoint[InLocation] =
    post("userInLocation" :: jsonBody[UserWithLocation]) { user: UserWithLocation =>
      logger.info("Received new request for /userInLocation")
      logger.debug(s"Received /userInLocation request for $user")
      isUserInLocation(user)(db) match {
        case scala.util.Success(inLocation) => Ok(inLocation)
        case scala.util.Failure(exc) =>
          BadRequest(new Exception(exc))
      }
    }

  private def isUserInLocation(user: UserWithLocation)(implicit db: DB): Try[InLocation] = {
    val loc = user.loc
    val userWithLocation = db.getUser(user.id)
    userWithLocation.flatMap { user =>
      val distance = GeoCalculus.distance(Location(user.lat, user.lon), loc)
      val e = db.getEforTile(Math.rint(loc.lat).toInt, Math.rint(loc.lon).toInt)
      e.map { e =>
        val inLocation = distance <= e
        InLocation(UserWithLocation(user.id, Location(user.lat, user.lon)), loc, status = inLocation)
      }
    }
  }

  def updateUserLocationEndpoint(db: DB): Endpoint[UserWithLocation] =
    put("updateLocation" :: jsonBody[UserWithLocation]) { user: UserWithLocation =>
      updateUserLocation(user)(db) match {
        case scala.util.Success(updatedUser) => Ok(updatedUser)
        case scala.util.Failure(exc) => BadRequest(new Exception(exc))
      }
    }

  private def updateUserLocation(userWithLocation: UserWithLocation)(implicit db: DB): Try[UserWithLocation] = {
    db.updateUserLocation(userWithLocation).map {
      case n if n == 1 => userWithLocation
      case _ => throw new Exception("User wasnt updated!")
    }
  }

  def locationService(db: DB): Service[Request, Response] =
    (userInLocationEndpoint(db) :+: updateUserLocationEndpoint(db) :+: usersCountInLocation(db))
      .toService

  def usersCountInLocation(db: DB): Endpoint[Int] =
    post("usersCountInLocation" :: jsonBody[Location]) { loc: Location =>
      getUsersCountInLocation(loc)(db) match {
        case Success(count) => Ok(count)
        case scala.util.Failure(exc) => BadRequest(new Exception(exc))
      }
    }

  implicit val e: finch.Encode.Aux[Exception, Application.Json] = Encode.instance[Exception, Application.Json]((exc, cs) =>
    Buf.Utf8(Json.fromString(exc.getMessage + exc.getStackTrace.mkString("\n")).toString())
  )

  private def getUsersCountInLocation(loc: Location)(implicit db: DB) = {
    db.getEforTile(loc.lat, loc.lon).flatMap { e =>
      val lat = GeoCalculus.possibleLats(loc.lat, e)
      val lon = GeoCalculus.possibleLons(loc.lat, loc.lon, e)

      db.getUsersInSquare(lat, lon).map { users: List[UserWithLocation] =>
        users.count(user => GeoCalculus.distance(user.loc, loc) <= e)
      }
    }
  }
}
