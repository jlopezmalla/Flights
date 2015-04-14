package com.stratio.model.spark

import com.stratio.model._
import com.stratio.utils.ParserUtils
import org.apache.spark.SparkContext._
import org.scalatest.{FlatSpec, ShouldMatchers}
import utils.LocalSparkSqlContext

class FlightDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import FlightDsl._

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
      date = ParserUtils.getDateTime(1987, 10, 14),
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
    val flight2 = flight1.copy(delay = delays2)
    val flight3= flight1.copy(date = ParserUtils.getDateTime(1988, 11, 14))
    val flight4= flight1.copy(origin = "SFO", dest = "SAN")

    val listFlights = List(flight1, flight2, flight3, flight4)
    val correctFlights = List(flightLine1, flightLine2, flightLine3, flightLine4)
    val textFlights = sc.parallelize(correctFlights)
  }

  trait WithErrorsFlightsText extends WithFlightsText{

    val flightErrorLine1 =
      "1987,13,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,12,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA"
    val flightErrorLine2 = "A,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,NA,1,NA,NA,NA,0"

    val errorFlights = correctFlights ++ List(flightErrorLine1, flightErrorLine2)
    val errorTextFlights = sc.parallelize(errorFlights)
  }

  trait WithFlightsInSeveralMonths extends WithDelays{

    val flight1 = Flight(
      date = ParserUtils.getDateTime(1987, 10, 14),
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
    val flight2 = flight1.copy(date = ParserUtils.getDateTime(1987, 10, 13), distance = 10885, dest = "MAS")
    val flight3= flight1.copy(date = ParserUtils.getDateTime(1988, 11, 14))
    val flight4= flight1.copy(origin = "SFO", date = ParserUtils.getDateTime(1987, 10, 13), distance = 330)
    val flight5= flight1.copy(origin = "SFO", date = ParserUtils.getDateTime(1988, 11, 13), distance = 330)

    val listPrices = sc.parallelize(List("1987,10,0.25", "1988,11,1.5"))
    val listFlights = List(flight1, flight2, flight3, flight4, flight5)
    val flights = sc.parallelize(listFlights)
  }


  trait WithGosthsFlights extends WithDelays{

    val flight1 = Flight(
      date = ParserUtils.getDateTime(1987, 10, 14),
      departureTime= 741,
      crsDepatureTime= 730,
      arrTime= -1,
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
      delay= delays1)//first Ghost fligthNumber = 1451
    val flight2 = flight1.copy(origin = "Aux1", departureTime = 800)//second Ghost fligthNumber = 1451
    val flight3= flight1.copy(origin = "Aux2", departureTime = 817)//second Ghost fligthNumber = 1451
    val flight4= flight1.copy(date = ParserUtils.getDateTime(1988, 11, 13), departureTime = 825)
    val flight5= flight1.copy(origin = "Aux3", departureTime = 825, arrTime = 912)
    val flight6= flight1.copy(flightNum = -1)//Unresolvable ghost flight No more Flights flightNumber=-1
    val flight7= flight1.copy(flightNum = 1)//Unresolvable ghost flight No flight in elapsedTime flightNumber=1
    val flight8= flight7.copy(departureTime = 1002, arrTime = 912)

    val elapsedSeconds = 2400
    val listFlights = List(flight1, flight2, flight3, flight4, flight5, flight6, flight7, flight8)
    val flights = sc.parallelize(listFlights)
  }


  "FlightDsl" should "parser csv in Flights" in new WithFlightsText {
    val collect = textFlights.toFlight.collect
    collect.sameElements(listFlights) should be(true)
  }

  it should "get all the parsing errors" in new WithErrorsFlightsText {
    errorTextFlights.toErrors.count should be (5)
    errorTextFlights.toErrors.countByKey.size should be (2)
  }

  it should "calculate the flying average for each airport" in new WithFlightsInSeveralMonths  {
    val averages = flights.averageDistanceByAirport.collect
    averages.size should be (2)
    averages should contain (("SFO", 330.0f))
    averages should contain (("SAN", 3926.3333f))
  }

  it should "calculate the month with less fuel consumption by airport" in new WithFlightsInSeveralMonths  {
    val minPrices = flights.minFuelConsumptionByMonthAndAirport(listPrices).collect
    minPrices.size should be (2)
    minPrices should contain (("SFO", (1987.toShort, 10.toShort)))
    minPrices should contain (("SAN", (1988.toShort, 11.toShort)))
  }

  it should "assign the appropriate flight to each ghost flight" in new WithGosthsFlights  {
    val flightsWithGhostSolved = flights.asignGhostFlights(elapsedSeconds)
    val flightsWithGhostSolvedList = flightsWithGhostSolved.collect
    
    flightsWithGhostSolved.count should be (flights.count)
    flightsWithGhostSolvedList should contain (
      flight1.copy(origin = "SAN", dest="Aux1", arrTime = 800, cRSArrTime = 730))
    flightsWithGhostSolvedList should contain (
      flight1.copy(origin = "Aux1", dest="Aux2", departureTime = 800, arrTime = 817, cRSArrTime = 730))
    flightsWithGhostSolvedList should contain (
      flight1.copy(origin = "Aux2", dest="Aux3", departureTime = 817, arrTime = 825, cRSArrTime = 730))

  }

}
