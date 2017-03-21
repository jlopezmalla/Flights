package com.stratio.model

import com.stratio.utils.ParserUtils

sealed case class Cancelled(id: String) {
  override def toString: String = id
}

object OnTime extends Cancelled(id = "OnTime")

object Cancel extends Cancelled(id = "Cancel")

object Unknown extends Cancelled(id = "Unknown")

case class Delays(carrier: Cancelled,
                   weather: Cancelled,
                   nAS: Cancelled,
                   security: Cancelled,
                   lateAircraft: Cancelled)

case class Flight(date: java.util.Date,
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
                  delay: Delays) {


  def toFlightSql: FlightSql = FlightSql(this)

//  def departureDate: DateTime =
//  LocalDateTime.of(date.getYear, date.getMonth, date.getDay, date.getHours, date.getMinutes)
//    .hourOfDay
//      .setCopy(departureTime.toString.substring(0, departureTime.toString.length - 2)).minuteOfHour
//      .setCopy(departureTime.toString.substring(departureTime.toString.length - 2)).secondOfMinute.setCopy(0)
//
//  def arriveDate: DateTime =
//    new DateTime(date).hourOfDay
//      .setCopy(departureTime.toString.substring(0, departureTime.toString.length - 2)).minuteOfHour
//      .setCopy(departureTime.toString.substring(departureTime.toString.length - 2)).secondOfMinute.setCopy(0)
//      .plusMinutes(cRSElapsedTime)
//
//  def isGhost: Boolean = arrTime == -1
//
//
//  def solveGhosts(sortedFlights: Seq[Flight], timeWindow: Int): Seq[Flight] = {
//
//    def solveGhosts(sortedFlights: Seq[Flight], timeWindow: Int): Seq[Flight] = {
//      sortedFlights match {
//        case head :: Nil => Seq(head)
//        case flight :: flightsToAnalyze => {
//          val flightsExamined = solveGhosts(flightsToAnalyze, timeWindow)
//          if (!flight.isGhost) Seq(flight) ++ flightsExamined
//          else {
//            val head = flightsExamined.head
//            if (head.isGhost || flight.departureDate.plusSeconds(timeWindow).getMillis < head.departureDate.getMillis)
//              Seq(flight) ++ flightsExamined
//            else {
//              val copy: Flight = flight.copy(
//                arrTime = head.departureTime, dest = head.origin, cRSArrTime = head.crsDepatureTime)
//              Seq(copy) ++ flightsExamined
//            }
//          }
//        }
//      }
//    }
//
//    solveGhosts(sortedFlights, timeWindow)
//  }
}

object Flight {

  /**
   * Crea un objeto Flight a partir de array de String.
    *
    * @param fields
   * @return
   */
  def apply(fields: Array[String]): Flight = {
    // scalastyle:off magic.number
    val (firstChunk, secondChunk) = fields.splitAt(22)
    // scalastyle:on magic.number
    val Array(year, month, dayOfMonth, _, departureTime, crsDepartureTime, arrTime, cRSArrTime, uniqueCarrier,
    flightNum, _, actualElapsedTime, cRSElapsedTime, _, arrDelay, depDelay, origin, dest, distance, _, _, cancelled) =
      firstChunk

    val Array(cancellationCode, _, carrierDelay, weatherDelay, nASDelay, securityDelay, lateAircraftDelay) = secondChunk

    Flight(
      ParserUtils.getDate(year.toInt, month.toInt, dayOfMonth.toInt),
      departureTime.toInt,
      crsDepartureTime.toInt,
      arrTime.toInt,
      cRSArrTime.toInt,
      uniqueCarrier.toString,
      flightNum.toInt,
      actualElapsedTime.toInt,
      cRSElapsedTime.toInt,
      arrDelay.toInt,
      depDelay.toInt,
      origin.toString,
      dest.toString,
      distance.toInt,
      parseCancelled(cancelled),
      cancellationCode.toInt,
      Delays(parseCancelled(carrierDelay), parseCancelled(weatherDelay), parseCancelled(nASDelay),
        parseCancelled(securityDelay), parseCancelled(lateAircraftDelay)))

  }

  /**
   * Extrae los errores de parseo de un CSV para convertilos al tipo Flight
    *
    * @param fields
   * @return
   */
  def extractErrors(fields: Array[String]): Seq[String] = {
    // scalastyle:off magic.number
    val (firstChunk, secondChunk) = fields.splitAt(22)
    // scalastyle:on magic.number
    val Array(year, month, dayOfMonth, _, departureTime, crsDepartureTime, arrTime, cRSArrTime, _,
    flightNum, _, actualElapsedTime, cRSElapsedTime, _, arrDelay, depDelay, _, _, distance, _, _, _) = firstChunk

    val Array(cancellationCode, _, _, _, _, _, _) = secondChunk

    val intsToValidate = Seq(year, month, dayOfMonth, departureTime, crsDepartureTime, arrTime, cRSArrTime, flightNum,
      actualElapsedTime, cRSElapsedTime, arrDelay, depDelay, distance, cancellationCode)

    val datesToValidate = Seq(year + "-" + month + "-" + dayOfMonth)

    intsToValidate.flatMap(ParserUtils.parseIntError(_)) ++ datesToValidate.flatMap(ParserUtils.parseDate(_))
  }

  /**
   * Parse un String al enumerado Cancelled :
   *   if field == 1 -> Cancel
   *   if field == 0 -> OnTime
   *   if field <> 0 && field<>1 -> Unknown
    *
    * @param field
   * @return
   */
  def parseCancelled(field: String): Cancelled = {
    field match {
      case "0" => OnTime
      case "1" => Cancel
      case _ => Unknown
    }
  }
}

case class FlightSql(date: java.sql.Date,
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

object FlightSql {
  def apply(f: Flight): FlightSql = {
    FlightSql(
      new java.sql.Date(f.date.getTime),
      f.departureTime,
      f.crsDepatureTime,
      f.arrTime,
      f.cRSArrTime,
      f.uniqueCarrier,
      f.flightNum,
      f.actualElapsedTime,
      f.cRSElapsedTime,
      f.arrDelay,
      f.depDelay,
      f.origin,
      f.dest,
      f.distance,
      f.cancelled,
      f.cancellationCode,
      f.delay
    )
  }
}
