package com.stratio.model.spark.sql

import com.stratio.model.Flight
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String]) {

  val sc = self.sparkContext
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  /**
   *
   * Parser the csv file and conver it to a DataFrame
   *
   */
  def toDataFrame: DataFrame = {
    self.map(line => Flight(line.split(",")).toFlightSql).toDF().cache()
  }


}

class FlightFunctions(self: DataFrame) {

  import self.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  /**
   *
   * Obtain the minimum fuel's consumption using a external DataFrame
   * with the fuel price by Year, Month
   *
   */
  def minFuelConsumptionByMonthAndAirport(fuelPrice: DataFrame): DataFrame = {

    self
      .select(year('date) as 'year, month('date) as 'month, 'origin, 'distance)
      .join(fuelPrice,"year" :: "month" :: Nil)
      .groupBy('origin, 'month, 'year).agg(sum('price * 'distance) as "sum")
      .orderBy('sum asc).limit(2)
  }

  /**
   *
   * Obtain the average distance flown by airport, taking the origin field as the airport to group
   *
   */
  def averageDistanceByAirport: DataFrame = {
    self.select('origin, 'distance).groupBy('origin).agg(avg('distance) as "avg")
  }

}


trait FlightSqlDsl {

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: DataFrame): FlightFunctions = new FlightFunctions(flights)
}

object FlightSqlDsl extends FlightSqlDsl

