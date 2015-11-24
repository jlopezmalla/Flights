package com.stratio.model.spark.streaming

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.stratio.model._
import com.stratio.utils.ParserUtils
import org.apache.spark.streaming.dstream.DStream

class FlightTicketStreamingTest extends StreamingSuiteBase {

  trait WithDelays{
    val delays1 = Delays(Unknown, Unknown, Unknown, Unknown, Unknown)
    val delays2 = delays1.copy(carrier = Cancel, lateAircraft = OnTime)
  }

  object WithFlights extends WithDelays{

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

  object WithQueuedRDD {

    val passenger1 = Person("Jorge", 'H', 30, Some(750.5f))
    val passenger2 = Person("Maria", 'F', 50, Some(1250.0f))
    val passenger3 = Person("Gaspar", 'H', 12)
    val ticket11 = FlightTicket(1, passenger1, Company)
    val ticket12 = ticket11.copy(passenger = passenger2, payer = Personal)
    val ticket13 = ticket11.copy(passenger = passenger3)
    val ticket21 = FlightTicket(2, passenger1, Personal)
    val ticket22 = ticket21.copy(passenger = passenger2, payer = Personal)
    val flight1Tickets = List(List(), List(ticket11), List(ticket22),
      List(ticket13, ticket21), List(ticket12), List(), List(ticket12), List(ticket13, ticket21))

  }

  test("calculate the ticket average by airport each 2 Seconds with a 4 second slide") {
    import FlightTicketDsl._
    val input = WithQueuedRDD.flight1Tickets
//    val expected = input.map(_.map(_.toString()))
    val expected = List(List(("1.0f", 2.0f)), List(), List())
    testOperation(input, (s: DStream[FlightTicket]) => s.avgAgeByWindow(3, 2), expected, 7, false)
  }
}