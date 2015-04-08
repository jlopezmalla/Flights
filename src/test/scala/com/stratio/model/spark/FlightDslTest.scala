package com.stratio.model.spark

import org.scalatest.{FlatSpec, ShouldMatchers}
import utils.LocalSparkSqlContext

class FlightDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import FlightDsl._

  trait WithFlightsText {

    val flightLine1 = "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA"
    val flightLine2 = "1987,10,18,7,729,730,847,849,PS,1451,NA,78,79,NA,-2,-1,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA"
    val flightLine3 = "1987,10,15,4,729,730,903,849,PS,1451,NA,94,79,NA,14,-1,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA"
    val flightLine4 = "1987,10,17,6,741,730,918,849,PS,1451,NA,97,79,NA,29,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA"

    val textFlights = sc.parallelize(List(flightLine1, flightLine2, flightLine3, flightLine4))
  }

  "FlightDsl" should "parser csv in Fligths" in new WithFlightsText (){

  }

}
