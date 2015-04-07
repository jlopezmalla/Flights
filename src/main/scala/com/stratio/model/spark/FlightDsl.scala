package com.stratio.model.spark

import com.stratio.model.Flight
import org.apache.spark.rdd.RDD

class FlightDsl {
  class FlightCsvReader(self: RDD[String]) {

    def toFlight: RDD[Flight] = ???

    def toErrors: RDD[(String, String)] = ???
  }

  class FlightFunctions(self: RDD[Flight]) {

    def minFuelConsumptionByMonthAndAirport(fuelPrize: RDD[String]): RDD[(String, Short)] = ???

    def averageDistanceBy
  }
}
