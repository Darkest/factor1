package ru.darkest.factor1.geoservice.db

import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import ru.darkest.factor1.geoservice.DB
import ru.darkest.factor1.geoservice.Schema.User
import ru.darkest.factor1.geoservice.services.dao.{Location, UserWithLocation}

trait withH2 {
  //val h2 = Server.createTcpServer("-tcpAllowOthers").start()
  val dbconf = ConfigFactory.parseString(
    """
      |  driver: "org.h2.Driver"
      |  url: "jdbc:h2:mem:"
      |  url1: "jdbc:h2:tcp://0.0.0.0:9092/~/test"
      |  user: "sa"
      |  password: ""
    """.stripMargin
  )
  val db = new DB(dbconf)
}

class DBTests extends FlatSpec {

  "CreateSchema" should "correctly create schema in DB" in new withH2 {
    assert(db.createSchema())
  }

  "updateUserLocation" should "change location for existing user" in new withH2 {
    db.createSchema()
    val result: (Int, Int) = db.loadUsersFromFile(getClass.getResource("/Users.db").getPath)
    val getUser = db.getUser(101)
    val insertUser = db.updateUserLocation(UserWithLocation(101, Location(123.123, 321.321)))
    assert(insertUser.isSuccess)
  }

  "updateUserLocation" should "add new user" in new withH2 {
    db.createSchema()
    val insertUser = db.updateUserLocation(UserWithLocation(101, Location(123.123, 321.321)))
    val insertUser2 = db.updateUserLocation(UserWithLocation(101, Location(124.123, 321.321)))
    val getUser = db.getUser(101)
    assert(insertUser.isSuccess)
  }

  "getUser" should "return added user" in new withH2 {
    db.createSchema()
    val insertUser = db.updateUserLocation(UserWithLocation(101, Location(123.123, 321.321)))
    assert(insertUser.isSuccess)
    val user = db.getUser(101)
    assert(user.get == User(101, 123.123, 321.321))
  }

  "loadUsers" should "load all the strings contained in file to DB" in new withH2 {
    db.createSchema()
    val result: (Int, Int) = db.loadUsersFromFile(getClass.getResource("/Users.db").getPath)
    assert(result._1 == result._2)
  }

  "loadLocations" should "load all the strings contained in file to DB" in new withH2 {
    db.createSchema()
    val result: (Int, Int) = db.loadLocationsFromFile(getClass.getResource("/Locations.db").getPath)
    assert(result._1 == result._2)
  }

}
