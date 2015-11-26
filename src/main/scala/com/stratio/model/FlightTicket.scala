package com.stratio.model

sealed case class PayerType (id: String) {override def toString: String = id}

object Company extends PayerType (id ="Company")
object Personal extends PayerType (id ="Personal")

case class Person(name: String,
  gender: Char,
  age: Short,
  revenue: Option[Float] = None)

case class  FlightTicket (flightNumber: Int,
  passenger: Person,
  payer: PayerType)
