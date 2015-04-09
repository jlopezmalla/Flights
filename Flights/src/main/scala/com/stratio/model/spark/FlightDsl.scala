package com.stratio.model.spark

import scala.language.implicitConversions
import com.stratio.model.Flight
import org.apache.spark.rdd.RDD

class FlightCsvReader(self: RDD[String]) {

    /**
     *
     * Parser the csv file with the format described in te readme.md file to a Fligth class
     *
     */
    def toFlight: RDD[Flight] = ???

    /**
     *
     * Obtain the parser errors
     *
     */
    def toErrors: RDD[(String, String)] = ???
  }

  class FlightFunctions(self: RDD[Flight]) {

    /**
     *
     * Obtain the minimum fuel's consumption using a external RDD with the fuel price by Month
     *
     */
    def minFuelConsumptionByMonthAndAirport(fuelPrice: RDD[String]): RDD[(String, Short)] = ???

    /**
     *
     * Obtain the average distance fliyed by airport, taking the origin field as the airport to group
     *
     */
    def averageDistanceByAirport: RDD[(String, Float)] = ???
  }


trait FlightDsl {

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: RDD[Flight]): FlightFunctions = new FlightFunctions(flights)
}

object FlightDsl extends FlightDsl

