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
  def apply(fields: Array[String]): Flight = {

    val (firstChuck, secondChuck) = fields.splitAt(22)
    val Array(year, month, dayofMonth,  dayOfWeek, departureTime, crsDepatureTime, arrTime, cRSArrTime,
    uniqueCarrier, flightNum,  actualElapsedTime, cRSElapsedTime, airTime, arrDelay,
    depDelay, origin, dest, distance, cancelled) = firstChuck
    val Array(cancellationCode, diverted, carrier, weather, nAS, security,
    lateAircraft) = secondChuck

    val delays = Delays(parseCancelled(carrier),parseCancelled(weather),parseCancelled(nAS),parseCancelled(security),

      parseCancelled(lateAircraft))

    Flight(
      ParserUtils.getDateTime(year.toInt,month.toInt,dayofMonth.toInt),
      departureTime.toInt,
      crsDepatureTime.toInt,
      arrTime.toInt,
      cRSArrTime.toInt,
      uniqueCarrier,
      flightNum.toInt,
      actualElapsedTime.toInt,
      cRSElapsedTime.toInt,
      airTime.toInt,
      arrDelay.toInt,
      depDelay.toInt,
      origin,
      dest,
      distance.toInt,
      parseCancelled(cancelled),
      cancellationCode.toInt,
      diverted,
      delays
    )
  }
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
  def parseCancelled(field: String): Cancelled = field.toString match {



  case "1" => Cancel
  case "0" => OnTime
  case  _   => Unknown



  }

//    if (!field.isEmpty) {
//
//      if (field.compareTo("1") == 0)
//        Cancel
//      if (field.compareTo("0") == 0)
//        OnTime
//
//    } else Unknown
//  }


}
