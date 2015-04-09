package com.stratio.model

import com.stratio.utils.ParserUtils
import org.joda.time.DateTime

sealed class Cancelled

object OnTime extends Cancelled
object Cancel extends Cancelled
object Unknown extends Cancelled

case class Delays (
                    carrier: Cancelled,
                    weather: Cancelled,
                    nAS: Cancelled,
                    security: Cancelled,
                    lateAircraft: Cancelled)

case class Flight (date: DateTime, //Tip: Use ParserUtils.getDateTime
                   dayOfWeek: Int,
                   departureTime: Int,
                   crsDepatureTime: Int,
                   arrTime: Int,
                   cRSArrTime: Int,
                   uniqueCarrier: String,
                   flightNum: Int,
                   actualElapsedTime: Int,
                   cRSElapsedTime: Int,
                   airTime: Int,
                   arrDelay: Int,
                   depDelay: Int,
                   origin: String,
                   dest: String,
                   distance: Int,
                   cancelled: Cancelled,
                   cancellationCode: Int,
                   diverted: String,
                   delay: Delays)

object Flight {
  /*
  *
  * Create a new Flight Class from a CSV file
  *
  */
  def apply(fields: Seq[String]): Flight = {
    val (firstChunk, secondChunk) = fields.splitAt(22)
    val Seq(year, month, dayOfMonth, dayOfWeek, departureTime, crsDepatureTime, arrTime, cRSArrTime, uniqueCarrier,
    flightNum, _, actualElapsedTime, cRSElapsedTime, airTime, arrDelay, depDelay, origin, dest, distance, _, _, cancelled) = firstChunk

    val Seq(cancellationCode, diverted, carrierDelay, weatherDelay, nASDelay, securityDelay, lateAircraftDelay) = secondChunk

    Flight(
      ParserUtils.getDateTime(year.toInt,month.toInt, dayOfMonth.toInt),
      dayOfWeek.toInt,
      departureTime.toInt,
      crsDepatureTime.toInt,
      arrTime.toInt,
      cRSArrTime.toInt,
      uniqueCarrier.toString,
      flightNum.toInt,
      actualElapsedTime.toInt,
      cRSElapsedTime.toInt,
      airTime.toInt,
      arrDelay.toInt,
      depDelay.toInt,
      origin.toString,
      dest.toString,
      distance.toInt,
      parseCancelled(cancelled),
      cancellationCode.toInt,
      diverted.toString,
      Delays(parseCancelled(carrierDelay), parseCancelled(weatherDelay), parseCancelled(nASDelay), parseCancelled(securityDelay),
        parseCancelled(lateAircraftDelay)))
  }

  /*
   *
   * Extract the different types of errors in a string list
   *
   */
  def extractErrors(fields: Array[String]): Seq[String] = {
    val (firstChunk, secondChunk) = fields.splitAt(22)
    val Array(year, month, dayOfMonth, dayOfWeek, departureTime, crsDepatureTime, arrTime, cRSArrTime, uniqueCarrier,
    flightNum, _, actualElapsedTime, cRSElapsedTime, airTime, arrDelay, depDelay, origin, dest, distance, _, _, cancelled) = firstChunk

    val Array(cancellationCode, diverted, carrierDelay, weatherDelay, nASDelay, securityDelay, lateAircraftDelay) = secondChunk

    val errorsInt = Seq(year,month, dayOfMonth, dayOfWeek, departureTime, crsDepatureTime, arrTime, cRSArrTime, flightNum,
      actualElapsedTime, cRSElapsedTime, airTime, arrDelay, depDelay, distance,cancellationCode).flatMap(evaluarInt(_))
    val errorsEnum = Seq(cancelled.toString)

    errorsInt++errorsEnum
  }

  def evalInt(in: String): Int = {
    if(in == "NA") 2
    else
      try {
        Integer.parseInt(in.trim)
        0
      } catch {
        case e: NumberFormatException => 1
      }
  }
  def evaluarInt(elemento : String): Option[(String)] = {
    if(evalInt(elemento)==1) Some(("Error Tipo 1"))
    else if (evalInt(elemento)==2) Some(("Error Tipo 2"))
    else None
  }

  /*
  *
  * Parse String to Cancelled Enum:
  *   if field == 1 -> Cancel
  *   if field == 0 -> OnTime
  *   if field <> 0 && field<>1 -> Unknown
  */
  def parseCancelled(field: String): Cancelled = {
    try {
      Integer.parseInt(field.trim)
      if (field.toInt == 0) OnTime
      else if (field.toInt == 1) Cancel
      else Unknown
    }
    catch {
      case e: NumberFormatException => Unknown
    }
  }
}