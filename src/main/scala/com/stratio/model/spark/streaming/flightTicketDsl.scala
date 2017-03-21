package com.stratio.model.spark.streaming

import com.stratio.model.{AirportStatistics, Flight, FlightTicket}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import scala.language.implicitConversions

class FlightTicketFunctions(self: DStream[FlightTicket]){

  /**
   *
   * Obtener la edad media de los pasajeros cada "windowSeconds" segundos moviendo la ventana cada "slideSeconds"
   * segundos
   *
   * Tip: Usar avgFunc
    *
    * @param windowSeconds Segundos de ventana
   * @param slideSeconds Segundos de sliding
   */
  def avgAgeByWindow(windowSeconds: Int, slideSeconds: Int): DStream[(Float, Float)]= {

    val avgFunc: ((Float, Float), (Float, Float)) => (Float, Float) =
      (sumCounter1, sumCounter2) =>
      (((sumCounter1._1 * sumCounter1._2) + (sumCounter2._1 * sumCounter2._2)) / (sumCounter1._2 + sumCounter2._2),
        sumCounter1._2 + sumCounter2._2)

    self.map(ticket => (ticket.passenger.age.toFloat, 1f))
      .reduceByWindow(avgFunc,
        Seconds(windowSeconds),
        Seconds(slideSeconds))
  }

  /**
   *
   * Extraer el nombre del aeropuerto del ticket correspondiente a partir de la información de los vuelos
   *
   * @param flights RDD con los vuelos a los que pertenecen los tickets
   * @return
   *         DStream con la asocicacion del nombre del aerpuerto correspondinte a la sálida de cada ticket de vuelo y
   *         el propio ticket
   *
   * Tip: Cruzar por el fligthNum
   *
   * Tip: Usar la operación transform para usar la información de los vuelos.
   */
  def byAirport(flights: RDD[Flight]): DStream[(String, FlightTicket)] = {
    val airportFNum = flights.map(flight => (flight.flightNum, flight.origin))
    self.map(ticket => (ticket.flightNumber, ticket)).
      transform(ticketNumber => ticketNumber.join(airportFNum)).map{
      case (idAirport, (ticket, airportName)) => (airportName, ticket)
    }
  }

  /**
   * Obtener para cada ventana de tiempo definida por "windowSeconds" y "slideSeconds" cual es el aeropuerto con
   * mayor número de tickets.
   *
   * @param flights RDD con los vuelos a los que pertenecen los tickets
   * @param windowSeconds Segundos de ventana
   * @param slideSeconds Segundos de sliding
   * @return
   *         DStream con el nombre y número de tickets obtenidos en esta ventana del aeropuerto con mayor número de
   *         tickets.
   *
   * Tip: Usar la función anterior "byAirport" para poder analizar la información por aeropuerto en cada ventana.
   *
   * * Tip: Si queremos hacer un reduceByKey con los datos de una ventana de tiempo, deberemos usar la operación
   * reduceByKeyAndWindow (usar la operacion reduceFunc)
   *
   * Tip: Si tenemos varios resultados en un DStream y queremos devolver un solo resultado en una venta tendremos que
   * usar la operacion reduce.
   */
  def airportMaxFlightsByWindow(flights: RDD[Flight], windowSeconds: Int, slideSeconds: Int): DStream[(String, Int)]= {
    val reduceFunc: (Int, Int) => Int = _ + _

    byAirport(flights).mapValues(_ => 1)
      .reduceByKeyAndWindow(reduceFunc, Seconds(windowSeconds), Seconds(windowSeconds))
      .reduce{case ((airport1, counter1), (airport2, counter2)) =>
        if(counter1 > counter2) (airport1, counter1) else (airport2, counter2)}
  }

  /**
   *
   * Obtener las estadísticas de cada aeropuerto a partir de la información de los tickets de vuelos.
   *
   * @param flights RDD con los vuelos a los que pertenecen los tickets
   * @return
   *         DStream con el nombre del aeropuerto y las estadisticas reflejadas con el objeto "AirportStatistics"
   *
   * Tip: Usar la función anterior "byAirport" para poder analizar la información por aeropuerto en cada ventana.
   *
   * Tip: Para obterner las estadisticas de cada aeropuerto debemos mantener un estado asociado a ellas para cada
   * micro-batch, usar la operacion state-ful "updateStateByKey"
   *
   * Tip: Usar la función "addFlightTickets" para actulizar la información estadistica de cada aeropuerto en cada
   * micro-batch
   */
  def airportStatistics(flights: RDD[Flight]): DStream[(String, AirportStatistics)]= {
    byAirport(flights).updateStateByKey((tickets: Seq[FlightTicket],
                                         airportStatistic: Option[AirportStatistics]) =>
    {airportStatistic match{
        case Some(statistic) => Some(statistic.addFlightTickets(tickets))
        case None => {
          val statistic = AirportStatistics(tickets.head)
          Some(statistic.addFlightTickets(tickets.tail))
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
