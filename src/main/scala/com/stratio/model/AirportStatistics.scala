package com.stratio.model

import com.stratio.utils.MapUtils

sealed case class RevenueInterval (id: String) {override def toString: String = id}
object Low extends RevenueInterval (id ="Low")
object Medium extends RevenueInterval (id ="Medium")
object High extends RevenueInterval (id ="High")

sealed case class AgeInterval (id: String) {override def toString: String = id}
object Child extends AgeInterval (id ="Child")
object Young extends AgeInterval (id ="Young")
object Adult extends AgeInterval (id ="Adult")
object Elder extends AgeInterval (id ="Elder")

case class AirportStatistics (
 genderCounter: Map[Char, Int],
 revenueCounter: Map[RevenueInterval, Int],
 ageCounter: Map[AgeInterval, Int],
 payerCounter: Map[PayerType, Int]){

  def addFlightTicket(ticket: FlightTicket): AirportStatistics = {
    copy(
      genderCounter = MapUtils.updateMap[Char](genderCounter, ticket.passenger.gender),
      ageCounter = MapUtils.updateMap[AgeInterval](ageCounter,
        AirportStatistics.extractAgeInterval(ticket.passenger.age)),
      revenueCounter = MapUtils.updateMapWithOption[RevenueInterval](revenueCounter,
        AirportStatistics.extractRevenueInterval(ticket.passenger.revenue)),
      payerCounter = MapUtils.updateMap[PayerType](payerCounter, ticket.payer))
  }

  def addFlightTickets(tickets: Seq[FlightTicket]): AirportStatistics = {
    if (tickets.isEmpty) this
    else tickets.aggregate(this)(_.addFlightTicket(_), _.aggregate(_))
  }

  def aggregate(another: AirportStatistics): AirportStatistics = {
    copy(
      genderCounter = MapUtils.aggregateMaps(genderCounter, another.genderCounter),
      ageCounter = MapUtils.aggregateMaps(ageCounter, another.ageCounter),
      revenueCounter = MapUtils.aggregateMaps(revenueCounter, another.revenueCounter),
      payerCounter = MapUtils.aggregateMaps(payerCounter, another.payerCounter))
  }
}

object AirportStatistics{
  def apply(ticket: FlightTicket): AirportStatistics = {
    val revenue = MapUtils.updateMapWithOption[RevenueInterval](Map(), extractRevenueInterval(ticket.passenger.revenue))
    new AirportStatistics(
      genderCounter =  Map(ticket.passenger.gender -> 1),
      revenueCounter = revenue,
      ageCounter = Map(extractAgeInterval(ticket.passenger.age) -> 1),
      payerCounter = Map(ticket.payer -> 1))
  }

  def extractAgeInterval(age: Int): AgeInterval = age match {
    case child if age <= 12 => Child
    case young if age <= 20 => Young
    case adult if age <= 65 => Adult
    case _ => Elder
  }

  def extractRevenueInterval(revenue: Option[Float]): Option[RevenueInterval] = revenue match {
    case None => None
    case Some(low) if low <= 150.0 => Some(Low)
    case Some(medium) if medium <= 300.0 => Some(Medium)
    case Some(high) => Some(High)
  }
}
