package com.stratio.model.spark

import java.util.Calendar

import com.stratio.model.{FuelPrice, Flight}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String]) {

  /**
   *
   * Parsea el RDD[String] de CSV's a un RDD[Flight]
   *
   * Tip: Usar método flatmap.
   *
   */
  def toFlight: RDD[Flight] = self.flatMap(line => {
    val fields = line.split(",")
    val errors = Flight.extractErrors(fields)
    if (errors.isEmpty) Seq(Flight(fields))
    else Seq()
  })

  /**
   *
   * Obtener todos los errores del CSV asociados a cada línea. OJO puede haber más de un error por línea
   *
   * Tip: Usar método flatmap.
   * Tip: Usar el método extractErrors de Flight
   *
   */
  def toErrors: RDD[(String, String)] = self.flatMap(line => {
    val fields = line.split(",")
    val errors = Flight.extractErrors(fields)
    if (errors.nonEmpty) errors.map((line, _)) else Seq()
  })
}

class FlightFunctions(self: RDD[Flight]) {

  /**
   *
   * Obtener la distancia media recorrida por cada aeropuerto.
   *
   *
   */
  def averageDistanceByAirport: RDD[(String, Float)] =
    self.map(flight => (flight.origin, flight.distance))
    .aggregateByKey((0, 0))(
      (combiner: (Int, Int), value: Int) => (combiner._1 + value, combiner._2 + 1),
      (combiner1: (Int, Int), combiner2: (Int, Int)) => (combiner1._1 + combiner2._1, combiner1._2 + combiner2._2))
    .mapValues{case (sum, counter) => sum.toFloat / counter.toFloat}

  /**
   *
   * Obtener el consumo mínimo por aeropuerto, mes y año.
   * @param fuelPrice RDD que contiene el precio del Fuel en un año
   *                  y mes determinado. Ver case class {@see com.stratio.model.FuelPrice}
   *
   *  Tip: Primero agrupar para cada aeropuerto, mes y año y sumar las distancias de los vuelos por el precio de
   *  combustible para ese mes y año y luego ver, para cada aeropuerto cual es el menor de los totales de los meses, año
   *
   *  Tip: Si el RDD es muy pequeño, podeis usar variables compartidas para evitar Joins
   *
   */
  def minFuelConsumptionByMonthAndAirport(fuelPrice: RDD[FuelPrice]): RDD[(String, (Int, Int))] = {
    val fuelBc = self.sparkContext.broadcast(fuelPrice.map(fuelPrice => {
      ((fuelPrice.year, fuelPrice.month), fuelPrice.price)
    }).collectAsMap)

    self.map(flight => {
      val cal = Calendar.getInstance()
      cal.setTime(flight.date)
      val keyM = (cal.get(Calendar.YEAR).get, cal.get(Calendar.MONTH).get)
      ((keyM, flight.origin), flight.distance * fuelBc.value.getOrElse(keyM, 1D))
    })
      .reduceByKey(_ + _)
      .map { case ((yearMonth, origin), fuelConsumptionPrize) =>
        (origin, (yearMonth, fuelConsumptionPrize))
      }
      .reduceByKey {
        case ((month1, fuel1), (month2, fuel2)) =>
          if (fuel1 <= fuel2) (month1, fuel1) else (month2, fuel2)
      }
      .map { case (airport, (month, fuel)) =>
        (airport, month)
      }
  }

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
   * window time = 600 (10 minutes)
   * flights before resolving ghostsFlights:
   * flight1 = flightNumber=1, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
   * flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
   * flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
   * flight4 = flightNumber=2, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
   * flight5 = flightNumber=2, departureTime=1-1-2015 deparHour821 arrTime=855 orig=A dest=D
   * flights after resolving ghostsFlights:
   * flight1 = flightNumber=1, departureTime=1-1-2015 deparHour810 arrTime=815 orig=A dest=B
   * flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
   * flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
   * flight4 = flightNumber=2, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
   * flight5 = flightNumber=2, departureTime=1-1-2015 deparHour821 arrTime=855 orig=A dest=D
   */

  def assignGhostsFlights(elapsedSeconds: Int): RDD[Flight] = {
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
