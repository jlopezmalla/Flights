package com.stratio.model.spark.streaming

import com.stratio.model.{AirportStatistics, Flight, FlightTicket}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

class FlightTicketFunctions(self: DStream[FlightTicket]){

  def userAvgByWindow(flights: RDD[Flight], windowSeconds: Int): DStream[(String, Float)]= {
    byAirport(flights).mapValues(_ => (1f, 1f)).reduceByKeyAndWindow(
        (acc, newValue) => ((acc._1 * acc._2) + (newValue._1 * newValue._2), acc._2 + newValue._2),
        Seconds(windowSeconds)).map(tuple => (tuple._1, tuple._2._1))
  }

  def byAirport(flights: RDD[Flight]): DStream[(String, FlightTicket)] = {
    val airportFNum = flights.map(flight => (flight.flightNum, flight.origin))
    self.map(ticket => (ticket.flightNumber, ticket)).
      transform(ticketNumber => ticketNumber.join(airportFNum)).map(tuple => (tuple._2._2, tuple._2._1))
  }

  def airportStatistics(flights: RDD[Flight]): DStream[(String, AirportStatistics)]= {
    byAirport(flights).updateStateByKey((tickets: Seq[FlightTicket], airportStatistic: Option[AirportStatistics]) =>{
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

  implicit def ticketFunctions(tickets: DStream[FlightTicket]):
    FlightTicketFunctions = new FlightTicketFunctions(tickets)
}

object FlightTicketDsl extends FlightTicketDsl
