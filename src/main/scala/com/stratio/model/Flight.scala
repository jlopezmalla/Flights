package com.stratio.model

import com.stratio.utils.ParserUtils
import org.joda.time.DateTime

sealed case class Cancelled (id: String) {override def toString: String = id}

object OnTime extends Cancelled (id ="OnTime")
object Cancel extends Cancelled (id ="Cancel")
object Unknown extends Cancelled (id ="Unknown")

case class Delays (
    carrier: Cancelled,
    weather: Cancelled,
    nAS: Cancelled,
    security: Cancelled,
    lateAircraft: Cancelled)

case class Flight (date: DateTime, //Tip: Use ParserUtils.getDateTime
    departureTime: Int,
    crsDepatureTime: Int,
    arrTime: Int,
    cRSArrTime: Int,
    uniqueCarrier: String,
    flightNum: Int,
    actualElapsedTime: Int,
    cRSElapsedTime: Int,
    arrDelay: Int,
    depDelay: Int,
    origin: String,
    dest: String,
    distance: Int,
    cancelled: Cancelled,
    cancellationCode: Int,
    delay: Delays)
{
  def isGhost: Boolean = arrTime == -1

  def departureDate: DateTime =
    date.hourOfDay.setCopy(departureTime.toString.substring(0, departureTime.toString.size - 2)).minuteOfHour
      .setCopy(departureTime.toString.substring(departureTime.toString.size - 2)).secondOfMinute.setCopy(0)

  def arriveDate: DateTime =
    date.hourOfDay.setCopy(departureTime.toString.substring(0, departureTime.toString.size - 2)).minuteOfHour
      .setCopy(departureTime.toString.substring(departureTime.toString.size - 2)).secondOfMinute.setCopy(0)
      .plusMinutes(cRSElapsedTime)
}

object Flight{

  /*
  *
  * Create a new Flight Class from a CSV file
  *
  */
  def apply(fields: Array[String]): Flight = ???

  /*
   *
   * Extract the different types of errors in a string list
   *
   */
  def extractErrors(fields: Array[String]): Seq[String] = ???

  /*
  *
  * Parse String to Cancelled Enum:
  *   if field == 1 -> Cancel
  *   if field == 0 -> OnTime
  *   if field <> 0 && field<>1 -> Unknown
  */
  def parseCancelled(field: String): Cancelled = ???
}
