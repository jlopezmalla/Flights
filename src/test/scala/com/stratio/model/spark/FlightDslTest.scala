package com.stratio.model.spark

import org.scalatest.{FlatSpec, ShouldMatchers}
import utils.LocalSparkSqlContext

class FlightDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import FlightDsl._

  trait WithFlightsText {

    val flightLine1 = "Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay, 1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA, 1987,10,15,4,729,730,903,849,PS,1451,NA,94,79,NA,14,-1,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA, 1987,10,17,6,741,730,918,849,PS,1451,NA,97,79,NA,29,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA, 1987,10,18,7,729,730,847,849,PS,1451,NA,78,79,NA,-2,-1,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA"
    val flightLine2 = "560917079,420034122616618,1,866173010386736"
    val flightLine3 = "560917079,420034122616618,1,866173010386736,1404162529,1404162578,16,208,100.75.161.156," +
      "17.149.36.144,50098,5223,WEB1,84.23.99.177,84.23.99.162,,84.23.99.162,10.210.4.73,1,052C,,330B,,,," +
      "5320,5332,26,26,(null),(null),,,,87,833,"

    val textFlights = sc.parallelize(List(flightLine1, flightLine2, flightLine3))
  }

  "FlightDsl" should "parser csv in Fligths" in new WithFlightsText (){

  }

}
