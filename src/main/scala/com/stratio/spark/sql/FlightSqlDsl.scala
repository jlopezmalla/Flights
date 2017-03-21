package com.stratio.model.spark.sql

import com.stratio.model.{FlightSql, Flight}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String]) (implicit session: SparkSession){

  import session.implicits._
  /**
   *
   * Parsea el RDD[String] de CSV's a un DataFrame.
   *
   * Tip: Crear un Flight y usar el método to FlightSql.
   *
   */
  def toDataFrame: Dataset[FlightSql] = {
    ???
  }
}

class FlightFunctions(self: Dataset[FlightSql]) (implicit session: SparkSession) {

  import org.apache.spark.sql.functions._
  import session.implicits._
  /**
   *
   * Obtener la distancia media recorrida por cada aeropuerto.
   *
   * Tip: Para usar funciones de aggregación df.agg(sum('distance) as "suma"
   *
   */
  def averageDistanceByAirport: DataFrame = {
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
  def minFuelConsumptionByMonthAndAirport(fuelPrice: DataFrame): DataFrame = {
    ???
  }
}


trait FlightSqlDsl {

  implicit def flightParser(lines: RDD[String])
                           (implicit session: SparkSession): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: Dataset[FlightSql])
                              (implicit session: SparkSession): FlightFunctions = new FlightFunctions(flights)
}

object FlightSqlDsl extends FlightSqlDsl

