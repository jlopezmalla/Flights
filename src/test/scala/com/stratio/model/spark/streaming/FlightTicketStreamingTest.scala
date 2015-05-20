package com.stratio.model.spark.streaming

import com.stratio.model._
import com.stratio.utils.ParserUtils
import org.scalatest.{FlatSpec, ShouldMatchers}
import utils.LocalSparkStreamingContext

import scala.collection.mutable

class FlightTicketStreamingTest extends FlatSpec with ShouldMatchers with LocalSparkStreamingContext {

  import FlightTicketDsl._
  trait WithDelays{
    val delays1 = Delays(Unknown, Unknown, Unknown, Unknown, Unknown)
    val delays2 = delays1.copy(carrier = Cancel, lateAircraft = OnTime)
  }

  trait WithFlights extends WithDelays{

    val flight1 = Flight(
      date = ParserUtils.getDateTime(1987, 10, 14),
      departureTime= 741,
      crsDepatureTime= 730,
      arrTime= 912,
      cRSArrTime= 849,
      uniqueCarrier= "PS",
      flightNum= 1,
      actualElapsedTime= 91,
      cRSElapsedTime= 79,
      arrDelay= 23,
      depDelay= 11,
      origin= "SFO",
      dest= "SAN",
      distance= 447,
      cancelled= OnTime,
      cancellationCode= 0,
      delay= delays1)
    val flight2 = flight1.copy(flightNum = 2, origin= "SAN", dest= "SFO")

    val flights = sc.parallelize(List(flight1, flight2))
  }

  trait WithQueuedRDD extends WithFlights{
    val passenger1 = Person("Jorge", 'H', 30, Some(750.5f))
    val passenger2 = Person("Maria", 'F', 50, Some(1250.0f))
    val passenger3 = Person("Nacho", 'H', 12)
    val ticket11 = FlightTicket(1, passenger1, Company)
    val ticket12 = ticket11.copy(passenger = passenger2)
    val ticket13 = ticket11.copy(passenger = passenger3)
    val ticket21 = FlightTicket(2, passenger1, Personal)
    val ticket22 = ticket21.copy(passenger = passenger2, payer = Personal)
    val flight1Tickets = List(ticket11, ticket22, ticket13)
    val flight2Tickets = List(ticket21, ticket12)
    val queuedRDD = mutable.Queue(sc.parallelize(flight1Tickets), sc.parallelize(flight2Tickets))

    val stream = ssc.queueStream(queuedRDD, true)
    Thread.sleep(3000)
    queuedRDD.
    val queuedRDD1 = mutable.Queue(sc.parallelize(flight1Tickets), sc.parallelize(flight2Tickets))
    val stream = ssc.queueStream(queuedRDD1, true)
    var result = List[(String, AirportStatistics)]()
  }

  "FlightTicketDsl" should "calculate the ticket average by airport each 2 Seconds" in new WithQueuedRDD {

    val airportStatictics = stream.userAvgByWindow(flights)
    airportStatictics.foreachRDD(rdd => result = result ++ rdd.collect)

    ssc.start

    Thread.sleep(2000)
    result.foreach(println)

    1 should be (2)
  }

}
