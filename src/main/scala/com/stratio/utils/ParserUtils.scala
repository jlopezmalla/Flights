package com.stratio.utils

import java.util.{Locale, Calendar, Date}

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatterBuilder

import scala.util.{Failure, Try}

object ParserUtils {

  val NOT_PARSEABLE_INT_ERROR: String = "Int Not Parseable"
  val NOT_PARSEABLE_DATE_ERROR: String = "Date Not Parseable"

  val dateTimeFormat = new DateTimeFormatterBuilder()
    // scalastyle:off magic.number
    .appendYear(2, 4)
    // scalastyle:on magic.number
    .appendLiteral('-')
    .appendMonthOfYear(1)
    .appendLiteral('-')
    .appendDayOfMonth(1)
    .toFormatter()

  def getDateTime(year: Int, month: Int, day: Int): DateTime =
    new DateTime(year, month, day, 0, 0, 0, 0)


  def getDate(year: Int, month: Int, day: Int): Date = {
    val cal = Calendar.getInstance()
    cal.set(Calendar.YEAR, year)
    cal.set(Calendar.MONTH, month)
    cal.set(Calendar.DAY_OF_MONTH, day)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTime
  }

  def parseIntError(intToParse: String): Option[String] = intToParse.filter(!_.isDigit).isEmpty match{
    case true => None
    case _ => Some(NOT_PARSEABLE_INT_ERROR)
  }

  def parseDate(stringDate: String): Option[String] = {
    Try { DateTime.parse(stringDate, dateTimeFormat)} match {
      case Failure(_) => Some(NOT_PARSEABLE_DATE_ERROR)
      case _ => None
    }
  }


}
