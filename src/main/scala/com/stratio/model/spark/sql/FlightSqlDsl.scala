package com.stratio.model.spark.sql

import com.stratio.model.Flight
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String]) {

  /**
   * Creación de contexto para Spark SQL.
   */
  val sc = self.sparkContext
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  /**
   *
   * Parsea el RDD[String] de CSV's a un DataFrame.
   *
   * Tip: Crear un Flight y usar el método to FlightSql.
   *
   */
  def toDataFrame: DataFrame = {
    self.map(line => Flight(line.split(",")).toFlightSql).toDF().cache()
  }


}

class FlightFunctions(self: DataFrame) {

  import org.apache.spark.sql.functions._
  import self.sqlContext.implicits._

  /**
   *
   * Obtener la distancia media recorrida por cada aeropuerto.
   *
   * Tip: Para usar funciones de aggregación df.agg(sum('distance) as "suma"
   *
   */
  def averageDistanceByAirport: DataFrame = {
    self.select('origin, 'distance).groupBy('origin).agg(avg('distance) as "avg")
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

    self
      .select(year('date) as 'year, month('date) as 'month, 'origin, 'distance)
      .join(fuelPrice, "year" :: "month" :: Nil)
      .groupBy('origin, 'month, 'year).agg(sum('price * 'distance) as "sum")
      .groupBy('origin).agg(min('sum) as "sum")
      .orderBy('sum asc)
  }

}


trait FlightSqlDsl {

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: DataFrame): FlightFunctions = new FlightFunctions(flights)
}

object FlightSqlDsl extends FlightSqlDsl

