package com.stratio.model.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String]) {

  /**
   * Creación de contexto para Spark SQL.
   */
  val sc = self.sparkContext
  val sqlContext = new SQLContext(sc)

  /**
   *
   * Parsea el RDD[String] de CSV's a un DataFrame.
   *
   * Tip: Crear un Flight y usar el método to FlightSql.
   *
   */
  def toDataFrame: DataFrame = {
    ???
  }
}

class FlightFunctions(self: DataFrame) {

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

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: DataFrame): FlightFunctions = new FlightFunctions(flights)
}

object FlightSqlDsl extends FlightSqlDsl

