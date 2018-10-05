package ru.darkest.factor1.geoservice.geoCalculus

import ru.darkest.factor1.geoservice.services.dao.Location

object GeoCalculus {
  // Earth radius in meters
  val EarthRadius = 6371000
  val degreesInOneMeter: Double = {
    360.0 / EarthRadius
  }

  def distance(loc1: Location, loc2: Location): Double = {
    val dLat = degreesToRadians(loc2.lat - loc1.lat)
    val dLon = degreesToRadians(loc2.lon - loc1.lon)

    val lat1 = degreesToRadians(loc1.lat)
    val lat2 = degreesToRadians(loc2.lat)

    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    EarthRadius * c
  }

  def degreesToRadians(deg: Double): Double = {
    deg * Math.PI / 180
  }

  def possibleLats(lat: Double, distanceInMeters: Int): (Double, Double) = {
    val d = distanceInMeters * degreesInOneMeter
    val min = if (lat - d <= -180) -(lat + d) else lat - d
    val max = if (lat + d > 180)  -(lat - d) else lat + d
    (min, max)
  }

  def possibleLons(lat: Double, lon: Double, distanceInMeters: Int): (Double, Double) = {
    val d = radiansToDegrees(distanceInMeters / EarthRadius / Math.cos(degreesToRadians(lat)))
    val min = lon - d
    val max = lon + d
    (min, max)
  }

  def radiansToDegrees(rad: Double): Double = {
    rad / Math.PI * 180
  }

}


