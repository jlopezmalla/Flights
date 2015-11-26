package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait LocalSparkSqlContext extends BeforeAndAfterAll { self: FlatSpec =>

  @transient var sqc: SQLContext = _
  @transient var sc: SparkContext = _

  override def beforeAll {
    Logger.getRootLogger.setLevel(Level.ERROR)
    sqc = LocalSparkSqlContext.getNewLocalSqlContext(1, "test")
    sc = sqc.sparkContext
  }

  override def afterAll {
    sqc.sparkContext.stop()
    System.clearProperty("spark.driver.port")
  }
}

object LocalSparkSqlContext {

  def getNewLocalSqlContext(numExecutors: Int = 1, title: String): SQLContext =
    new SQLContext(getNewLocalContext(numExecutors, title))

  private def getNewLocalContext(numExecutors: Int = 1, title: String): SparkContext = {
    new SparkContext(s"local[$numExecutors]", title)
  }
}
