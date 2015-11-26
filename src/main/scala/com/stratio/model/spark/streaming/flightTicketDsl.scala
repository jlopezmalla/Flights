package com.stratio.model.spark.streaming

import scala.language.implicitConversions
import com.stratio.model.{AirportStatistics, Flight, FlightTicket}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

class FlightTicketFunctions(self: DStream[FlightTicket]){


  def avgAgeByWindow(windowSeconds: Int, slideSeconds: Int): DStream[(Float, Float)]= {
    self.map(ticket => (ticket.passenger.age.toFloat, 1f)).reduceByWindow(
    (value1, value2) => (((value1._1 * value1._2) + (value2._1 * value2._2))/(value1._2 + value2._2),
    value1._2 + value2._2), Seconds(windowSeconds), Seconds(slideSeconds))
  }

  def byAirport(flights: RDD[Flight]): DStream[(String, FlightTicket)] = {
    val airportFNum = flights.map(flight => (flight.flightNum, flight.origin))
    self.map(ticket => (ticket.flightNumber, ticket)).
      transform(ticketNumber => ticketNumber.join(airportFNum)).map(tuple => (tuple._2._2, tuple._2._1))
  }

  def airportMaxFlightsByWindow(flights: RDD[Flight], windowSeconds: Int): DStream[(String, Int)]= {
    byAirport(flights).mapValues(_ => 1).reduceByKeyAndWindow(_ + _, Seconds(windowSeconds))
//      .reduce((airportCounter1, airportCounter2) =>
//    if(airportCounter1._2 > airportCounter2._2) airportCounter1 else airportCounter2)
  }


  def airportStatistics(flights: RDD[Flight]): DStream[(String, AirportStatistics)]= {
    byAirport(flights).updateStateByKey((tickets: Seq[FlightTicket], airportStatistic: Option[AirportStatistics]) => {
      airportStatistic match{
        case Some(statistic) => Some(statistic.addFlightTickets(tickets, statistic))
        case None => {
          val statistic = AirportStatistics(tickets.head)
          Some(statistic.addFlightTickets(tickets.tail, statistic))
        }
      }
    })
  }
}

trait FlightTicketDsl {

  implicit def ticketFunctions(tickets: DStream[FlightTicket]): FlightTicketFunctions =
    new FlightTicketFunctions(tickets)
}

object FlightTicketDsl extends FlightTicketDsl
