package ru.darkest.factor1.geoservice.geoCalculus

import org.scalatest.FlatSpec
import ru.darkest.factor1.geoservice.services.dao.Location

class GeoCalculusTests extends FlatSpec{

  "GeoCalculus.distance" should "return zero for two identical points" in {
    assert(GeoCalculus.distance(Location(1.2345, 2.3456), Location(1.2345, 2.3456)) == 0)
  }

  "GeoCalculus.distance" should "return ~872220 meters between (1.2345, 6.7890) and (6.7890, 1.2345)" in {
    assert(GeoCalculus.distance(Location(1.2345, 6.7890), Location(6.7890, 1.2345)).floor == 872220)
  }

  "possibleLats" should "return positive start and negative end 1" in {
    val minus = GeoCalculus.possibleLats(-179.9, 10000)
    assert(minus._1 > 0 && minus._2<0)
  }

  "possibleLats" should "return positive start and negative end 2" in {
    val plus = GeoCalculus.possibleLats(179.9, 10000)
    assert(plus._1 > 0 && plus._2<0)
  }
}
