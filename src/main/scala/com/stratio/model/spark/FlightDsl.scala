package com.stratio.model.spark

import java.util.Calendar

import com.stratio.model.Flight
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String]) {

  /**
   *
   * Parser the csv file with the format described in te readme.md file to a Fligth class
   *
   */
  def toFlight: RDD[Flight] = toParsedFlight.filter(_.isRight).map(_.right.get)

  /**
   *
   * Intermediate step between the Raw file and the sanitized one
   *
   */
  def toParsedFlight: RDD[Either[(String, String), Flight]] = {
    self.flatMap(line => {
      val fields = line.split(",")
      val errors = Flight.extractErrors(fields)
      if (errors.isEmpty) Seq(Right(Flight(fields)))
      else errors.map(error => Left((error, line)))
    })
  }

  /**
   *
   * Obtain the parser errors
   *
   */
  def toErrors: RDD[(String, String)] = toParsedFlight.filter(_.isLeft).map(_.left.get)
}

class FlightFunctions(self: RDD[Flight]) {

  /**
   *
   * Obtain the minimum fuel's consumption using a external RDD with the fuel price by Year, Month
   *
   */
  def minFuelConsumptionByMonthAndAirport(fuelPrice: RDD[String]): RDD[(String, (Short, Short))] = {
    val fuelBc = self.sparkContext.broadcast(fuelPrice.map(_.split(",")).map(fields => {
      val Array(year, month, price) = fields
      ((year.toShort, month.toShort), price.toDouble)
    }).collectAsMap)

    self.map(flight => {
      val cal = Calendar.getInstance()
      cal.setTime(flight.date)
      val keyM = (cal.get(Calendar.YEAR).get.toShort, cal.get(Calendar.MONTH).get.toShort)
      ((keyM, flight.origin), flight.distance * fuelBc.value.getOrElse(keyM, 1D))
    }).reduceByKey(_ + _).map(distanceByYearMonthAirport =>
      (distanceByYearMonthAirport._1._2, (distanceByYearMonthAirport._1._1, distanceByYearMonthAirport._2)))
      .reduceByKey((monthFuel1, monthFuel2) => if (monthFuel1._2 <= monthFuel2._2) monthFuel1 else monthFuel2)
      .map(airportYearMonthFuel => (airportYearMonthFuel._1, airportYearMonthFuel._2._1))
  }

  /**
   *
   * Obtain the average distance fliyed by airport, taking the origin field as the airport to group
   *
   */
  def averageDistanceByAirport: RDD[(String, Float)] = self.map(flight => (flight.origin, flight.distance))
    .aggregateByKey((0, 0))((combiner: (Int, Int), value: Int) => (combiner._1 + value, combiner._2 + 1),
      (combiner1: (Int, Int), combiner2: (Int, Int)) => (combiner1._1 + combiner2._1, combiner1._2 + combiner2._2))
    .mapValues(sumCounter => sumCounter._1.toFloat / sumCounter._2.toFloat)


  /**
    * A Ghost Flight is each flight that has arrTime = -1 (that means that the flight didn't land where was supposed to
    * do it).
    *
    * The correct assign to those flights will be :
    *
    * The ghost's flight arrTime would take the data of the nearest flight in time that has its flightNumber and which
    * deptTime is lesser than the ghost's flight deptTime plus a configurable amount of seconds, from here we will call
    * this flight: the saneated Flight
    *
    * So if there is no sanitized Flight for a ghost Flight we will return the same ghost Flight but if there are some
    * sanitized Flight we had to assign the following values to the ghost flight:
    *
    * dest = sanitized Flight origin
    * arrTime = sanitized Flight depTime
    * csrArrTime = sanitized Flight csrDepTime
    * date =  sanitized Flight date
    *
    * --Example
    *   window time = 600 (10 minutes)
    *   flights before resolving ghostsFlights:
    *   flight1 = flightNumber=1, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
    *   flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
    *   flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
    *   flight4 = flightNumber=2, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
    *   flight5 = flightNumber=2, departureTime=1-1-2015 deparHour821 arrTime=855 orig=A dest=D
    *  flights after resolving ghostsFlights:
    *   flight1 = flightNumber=1, departureTime=1-1-2015 deparHour810 arrTime=815 orig=A dest=B
    *   flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
    *   flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
    *   flight4 = flightNumber=2, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
    *   flight5 = flightNumber=2, departureTime=1-1-2015 deparHour821 arrTime=855 orig=A dest=D
    */

  def asignGhostFlights(elapsedSeconds: Int): RDD[Flight] = {
    self.groupBy(_.flightNum).flatMapValues(
      groupedFlights => Flight.solveGhosts(groupedFlights.toList.sortBy(_.departureDate.getMillis),
        elapsedSeconds)).values
  }

}


trait FlightDsl {

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: RDD[Flight]): FlightFunctions = new FlightFunctions(flights)
}

object FlightDsl extends FlightDsl

