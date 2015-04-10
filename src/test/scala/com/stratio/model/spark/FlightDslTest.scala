package com.stratio.model.spark

import org.apache.spark.SparkContext._
import org.scalatest.{FlatSpec, ShouldMatchers}

import com.stratio.model._
import com.stratio.utils.ParserUtils
import utils.LocalSparkSqlContext

class FlightDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import FlightDsl._

  trait WithFlightsText {

    val flightLine1 = "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,0,NA,NA,NA,NA,NA,NA"
    val flightLine2 = "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,0,NA,1,NA,NA,NA,0"
    val flightLine3 = "1988,11,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,0,NA,NA,NA,NA,NA,NA"
    val flightLine4 = "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SFO,SAN,447,NA,NA,0,0,NA,NA,NA,NA,NA,NA"

    val delays1 = Delays(Unknown, Unknown, Unknown, Unknown, Unknown)
    val delays2 = delays1.copy(carrier = Cancel, lateAircraft = OnTime)

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
      "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,C,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA"
    val flightErrorLine2 = "A,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,NA,1,NA,NA,NA,0"

    val errorFlights = correctFlights ++ List(flightErrorLine1, flightErrorLine2)
    val errorTextFlights = sc.parallelize(errorFlights)
  }

  "FlightDsl" should "parser csv in Flights" in new WithFlightsText {
    textFlights.toFlight.collect.sameElements(listFlights) should be(true)
  }

  it should "get all the parsing errors" in new WithErrorsFlightsText {
    errorTextFlights.toErrors.count should be (4)
    errorTextFlights.toErrors.countByKey.size should be (2)
  }
}
