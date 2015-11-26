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

    val flightTickets = List(List(ticket11, ticket12), List(ticket13, ticket21), List(),List(ticket12), List(ticket12),
      List(ticket13, ticket21, ticket24), List(), List(ticket23))

    val expectedFlightByAirport = List(List((30.5f, 4.0f)), List((33.4f, 5.0f)), List((12.0f, 1.0f)))

    val expectedFlightByAirportSolution = List(
      List(("SFO", 3)),
      List(("SFO", 3)),
      List(("SAN", 1)))

    val statisticsStep1 = List(("SFO", AirportStatistics(
        Map('H' -> 1, 'F' -> 1),
        Map(High -> 2),
        Map(Adult -> 2),
        Map(Company -> 2))))

    val statisticsStep23 = List(("SFO", AirportStatistics(
        Map('H' -> 2, 'F' -> 1),
        Map(High -> 2),
        Map(Adult -> 2, Child -> 1),
        Map(Company -> 2, Personal -> 1))),
      ("SAN", AirportStatistics(
        Map('H' -> 1),
        Map(High -> 1),
        Map(Adult -> 1),
        Map(Company -> 1))))

    val statisticsStep4 = List(("SFO",   AirportStatistics(
        Map('H' -> 2, 'F' -> 2),
        Map(High -> 3),
        Map(Adult -> 3, Child -> 1),
        Map(Company -> 3, Personal -> 1))),
      ("SAN", AirportStatistics(
        Map('H' -> 1),
        Map(High -> 1),
        Map(Adult -> 1),
        Map(Company -> 1))))

    val statisticsStep5 = List(("SFO",   AirportStatistics(
      Map('H' -> 2, 'F' -> 3),
      Map(High -> 4),
      Map(Adult -> 4, Child -> 1),
      Map(Company -> 4, Personal -> 1))),
      ("SAN", AirportStatistics(
        Map('H' -> 1),
        Map(High -> 1),
        Map(Adult -> 1),
        Map(Company -> 1))))

    val statisticsStep67 = List(("SFO",   AirportStatistics(
      Map('H' -> 3, 'F' -> 3),
      Map(High -> 4),
      Map(Adult -> 4, Child -> 2),
      Map(Company -> 4, Personal -> 2))),
      ("SAN", AirportStatistics(
        Map('H' -> 2, 'F' -> 1),
        Map(High -> 2),
        Map(Adult -> 3),
        Map(Company -> 2, Personal -> 1))))

    val statisticsStep8 = List(("SFO", AirportStatistics(
      Map('H' -> 3, 'F' -> 3),
      Map(High -> 4),
      Map(Adult -> 4, Child -> 2),
      Map(Company -> 4, Personal -> 2))),
      ("SAN", AirportStatistics(
        Map('H' -> 3, 'F' -> 1),
        Map(High -> 2),
        Map(Adult -> 3, Child -> 1),
        Map(Company -> 2, Personal -> 2))))

    val expectedFlightAirpotStatics =
      List(statisticsStep1, statisticsStep23, statisticsStep23, statisticsStep4, statisticsStep5, statisticsStep67,
        statisticsStep67, statisticsStep8)
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

  test("get max flights by aiports each 3 seconds") {
    import FlightTicketDsl._
    val input = WithQueuedRDD.flightTickets
    val expected = WithQueuedRDD.expectedFlightByAirportSolution
    testOperation(input,
      (s: DStream[FlightTicket]) => s.airportMaxFlightsByWindow(WithFlights.flights, 3, 3),
      expected,
      9,
      false)
  }

  test("aggregate the airport statistics") {
    import FlightTicketDsl._
    val input = WithQueuedRDD.flightTickets
    val expected = WithQueuedRDD.expectedFlightAirpotStatics
    testOperation(input,
      (s: DStream[FlightTicket]) => s.airportStatistics(WithFlights.flights),
      expected,
      8,
      false)
  }
}