package com.stratio.model.spark.sql

import com.stratio.model._
import com.stratio.utils.ParserUtils
import org.scalatest.{FlatSpec, ShouldMatchers}
import utils.LocalSparkSession

import scala.language.implicitConversions

class FlightSqlTest extends FlatSpec with ShouldMatchers with LocalSparkSession {

  import FlightSqlDsl._

  import session.implicits._

  trait WithDelays{
    val delays1 = Delays(Unknown, Unknown, Unknown, Unknown, Unknown)
    val delays2 = delays1.copy(carrier = Cancel, lateAircraft = OnTime)
  }

  trait WithFlightsText extends WithDelays{

    val flightLine1 = "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,0,NA,NA,NA,NA,NA,NA"
    val flightLine2 = "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,0,NA,1,NA,NA,NA,0"
    val flightLine3 = "1988,11,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,0,NA,NA,NA,NA,NA,NA"
    val flightLine4 = "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SFO,SAN,447,NA,NA,0,0,NA,NA,NA,NA,NA,NA"


    val flight1 = Flight(
      date = ParserUtils.getDate(1987, 10, 14),
      departureTime= 741,
      crsDepatureTime= 730,
      arrTime= 912,
      cRSArrTime= 849,
      uniqueCarrier= "PS",
      flightNum= 1451,
      actualElapsedTime= 91,
      cRSElapsedTime= 79,
      arrDelay= 23,
      depDelay= 11,
      origin= "SAN",
      dest= "SFO",
      distance= 447,
      cancelled= OnTime,
      cancellationCode= 0,
      delay = delays1)
    val flight2 = flight1.copy(delay = delays2)
    val flight3= flight1.copy(date = ParserUtils.getDate(1988, 11, 14))
    val flight4= flight1.copy(origin = "SFO", dest = "SAN")

    val listFlights = List(flight1, flight2, flight3, flight4)
    val correctFlights = List(flightLine1, flightLine2, flightLine3, flightLine4)
    val textFlights = sc.parallelize(correctFlights)
  }

  class WithFlightsInSeveralMonths extends WithDelays{

    private val flight1 = Flight(
      date = ParserUtils.getDate(1987, 10, 14),
      departureTime= 741,
      crsDepatureTime= 730,
      arrTime= 912,
      cRSArrTime= 849,
      uniqueCarrier= "PS",
      flightNum= 1451,
      actualElapsedTime= 91,
      cRSElapsedTime= 79,
      arrDelay= 23,
      depDelay= 11,
      origin= "SAN",
      dest= "SFO",
      distance= 447,
      cancelled= OnTime,
      cancellationCode= 0,
      delay= delays1)
    private val flight2 = flight1.copy(date = ParserUtils.getDate(1987, 10, 13), distance = 10885, dest = "MAS")
    private val flight3= flight1.copy(date = ParserUtils.getDate(1988, 11, 14))
    private val flight4= flight1.copy(origin = "SFO", date = ParserUtils.getDate(1987, 10, 13), distance = 330)
    private val flight5= flight1.copy(origin = "SFO", date = ParserUtils.getDate(1988, 11, 13), distance = 330)

    private val prices: List[FuelPrice] = List(FuelPrice(1987, 11, 0.25d), FuelPrice(1988, 12, 1.5d))
    val listPrices = session.createDataFrame(prices)
    private val listFlights = List(flight1, flight2, flight3, flight4, flight5).map(_.toFlightSql)
    val flights = session.createDataset(listFlights)
  }

  "FlightDsl" should "parser csv in DataFrame" in new WithFlightsText {
    val flightsDF = textFlights.toDataFrame
    assert(flightsDF.count() == 4L)
  }

  it should "calculate the flying average for each airport" in new WithFlightsInSeveralMonths  {
    import FlightSqlDsl._
    val averages = flights.averageDistanceByAirport.collect()
    assert(averages.exists(_.get(0).equals("SAN"))
      && averages.find(_.get(0).equals("SAN")).get.get(1).equals(3926.3333333333335d))
    assert(averages.exists(_.get(0).equals("SFO"))
      && averages.find(_.get(0).equals("SFO")).get.get(1).equals(330.0d))
  }

  it should "calculate the month with less fuel consumption by airport" in new WithFlightsInSeveralMonths  {
    val minPrices = flights.minFuelConsumptionByMonthAndAirport(listPrices).collect()
    assert(minPrices(0).get(0).equals("SFO") && minPrices(0).get(1).equals(82.5))
    assert(minPrices(1).get(0).equals("SAN") && minPrices(1).get(1).equals(670.5))
  }
}
