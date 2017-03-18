package com.stratio.model.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String]) (implicit session: SparkSession){

  /**
   *
   * Parsea el RDD[String] de CSV's a un DataFrame.
   *
   * Tip: Crear un Flight y usar el método to FlightSql.
   *
   */
  def toDataFrame: Dataset = {
    ???
  }
}

class FlightFunctions(self: Dataset) {

  /**
   *
   * Obtener la distancia media recorrida por cada aeropuerto.
   *
   * Tip: Para usar funciones de aggregación df.agg(sum('distance) as "suma"
   *
   */
  def averageDistanceByAirport: Dataset = {
    ???
  }

  /**
   *
   * Obtener el consumo mínimo por aeropuerto, mes y año.
    *
    * @param fuelPrice DataFrame que contiene el precio del Fuel en un año
   *                  y mes determinado. Ver case class {@see com.stratio.model.FuelPrice}
   *
   *  Tip: Se pueden utilizar funciones propias del estándar SQL.
   *  ej: Extraer el año de un campo fecha llamado date:  year('date)
   */
  def minFuelConsumptionByMonthAndAirport(fuelPrice: Dataset): Dataset = {
    ???
  }

}


trait FlightSqlDsl {

  implicit def flightParser(lines: RDD[String])
                           (implicit session: SparkSession): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: Dataset)
                              (implicit session: SparkSession): FlightFunctions = new FlightFunctions(flights)
}

object FlightSqlDsl extends FlightSqlDsl

