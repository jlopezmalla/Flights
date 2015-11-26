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
      date = ParserUtils.getDate(1987, 10, 14),
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
    val flight3 = flight1.copy(flightNum = 3, origin= "JFK", dest= "SFO")

    val flights = sc.parallelize(List(flight1, flight2))
  }

  object WithQueuedRDD {

    val passenger1 = Person("Jorge", 'H', 30, Some(750.5f))
    val passenger2 = Person("Maria", 'F', 50, Some(1250.0f))
    val passenger3 = Person("Gaspar", 'H', 12)
    val passenger4 = Person("Julia", 'F', 25)
    val ticket11 = FlightTicket(1, passenger1, Company)
    val ticket12 = ticket11.copy(passenger = passenger2)
    val ticket13 = ticket11.copy(passenger = passenger3, payer = Personal)
    val ticket21 = FlightTicket(2, passenger1, Company)
    val ticket22 = ticket21.copy(passenger = passenger2)
    val ticket23 = ticket21.copy(passenger = passenger3, payer = Personal)
    val ticket24 = ticket21.copy(passenger = passenger4, payer = Personal)
    val ticket31 = FlightTicket(3, passenger1, Personal)
    val ticket32 = ticket31.copy(passenger = passenger4)

    val flightTickets = List(List(ticket11, ticket12), List(), List(ticket13, ticket21), List(ticket12), List(ticket12),
      List(ticket13, ticket21, ticket24), List(), List(ticket23))

    val expectedFlightByAirport = List(List((30.5f, 4.0f)), List((50.0f, 2.0f)), List((19.75f, 4.0f)))

    val expectedFlightByAirportSolution = List(
      List(("SFO", 3)),
      List(("SFO", 2)),
      List(("SAN", 2)), List())
  }

  test("calculate the age average each 3 Seconds with a 3 second slide") {
    import FlightTicketDsl._
    val input = WithQueuedRDD.flightTickets
    val expected = WithQueuedRDD.expectedFlightByAirport
    testOperation(input,
      (s: DStream[FlightTicket]) => s.avgAgeByWindow(3, 3),
      expected,
      9,
      false)
  }

  test("group flights by aiport in each micro batch") {
    import FlightTicketDsl._
    val input = WithQueuedRDD.flightTickets
    val expected = List(List(), List(), List() ,List() ,List(), List())
    testOperation(input,
      (s: DStream[FlightTicket]) => s.byAirport(WithFlights.flights),
      expected,
      7,
      false)
  }


  test("get max flight every 3 seconds") {
    import FlightTicketDsl._
    val input = WithQueuedRDD.flightTickets
    val expected = List(List(), List(), List(), List(), List())
    testOperation(input,
      (s: DStream[FlightTicket]) => s.airportMaxFlightsByWindow(WithFlights.flights, 2),
      expected,
      7,
      false)
  }
}