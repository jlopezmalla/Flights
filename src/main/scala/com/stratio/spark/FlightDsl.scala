package com.stratio.model.spark

import java.util.Calendar

import com.stratio.model.{Flight, FuelPrice}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String])(implicit session: SparkSession) {

  /**
   *
   * Parsea el RDD[String] de CSV's a un RDD[Flight]
   *
   * Tip: Usar método flatmap.
   *
   */
  def toFlight: RDD[Flight] = {
    ???
  }

  /**
   *
   * Obtener todos los errores del CSV asociados a cada línea. OJO puede haber más de un error por línea
   *
   * Tip: Usar método flatmap.
   * Tip: Usar el método extractErrors de Flight
   *
   */
  def toErrors: RDD[(String, String)] = {
   ???
  }
}

class FlightFunctions(self: RDD[Flight]) {

  /**
   *
   * Obtener la distancia media recorrida por cada aeropuerto.
   *
   *
   */
  def averageDistanceByAirport: RDD[(String, Float)] = {
    ???
  }

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
   ???
  }
//  /**
//    * A Ghost Flight is each flight that has arrTime = -1 (that means that the flight didn't land where was supposed to
//    * do it).
//    *
//    * The correct assign to those flights will be :
//    *
//    * The ghost's flight arrTime would take the data of the nearest flight in time that has its flightNumber and which
//    * deptTime is lesser than the ghost's flight deptTime plus a configurable amount of seconds, from here we will call
//    * this flight: the saneated Flight
//    *
//    * So if there is no sanitized Flight for a ghost Flight we will return the same ghost Flight but if there are some
//    * sanitized Flight we had to assign the following values to the ghost flight:
//    *
//    * dest = sanitized Flight origin
//    * arrTime = sanitized Flight depTime
//    * csrArrTime = sanitized Flight csrDepTime
//    * date =  sanitized Flight date
//    *
//    * --Example
//    * window time = 600 (10 minutes)
//    * flights before resolving ghostsFlights:
//    * flight1 = flightNumber=1, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
//    * flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
//    * flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
//    * flight4 = flightNumber=2, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
//    * flight5 = flightNumber=2, departureTime=1-1-2015 deparHour821 arrTime=855 orig=A dest=D
//    * flights after resolving ghostsFlights:
//    * flight1 = flightNumber=1, departureTime=1-1-2015 deparHour810 arrTime=815 orig=A dest=B
//    * flight2 = flightNumber=1, departureTime=1-1-2015 departureTime=819 arrTime=1000 orig=C dest=D
//    * flight3 = flightNumber=1,  departureTime=1-1-2015 departureTime=815 arrTime=816 orig=B dest=C
//    * flight4 = flightNumber=2, departureTime=1-1-2015 deparHour810 arrTime=-1 orig=A dest=D
//    * flight5 = flightNumber=2, departureTime=1-1-2015 deparHour821 arrTime=855 orig=A dest=D
//    */
//
//  def assignGhostsFlights(elapsedSeconds: Int): RDD[Flight] = {
//    self.groupBy(_.flightNum).flatMapValues(
//      groupedFlights => Flight.solveGhosts(groupedFlights.toList.sortBy(_.departureDate.getMillis),
//        elapsedSeconds)).values
//  }
}


trait FlightDsl {

  implicit def flightParser(lines: RDD[String])(implicit session: SparkSession): FlightCsvReader =
    new FlightCsvReader(lines)

  implicit def flightFunctions(flights: RDD[Flight]): FlightFunctions = new FlightFunctions(flights)
}

object FlightDsl extends FlightDsl
