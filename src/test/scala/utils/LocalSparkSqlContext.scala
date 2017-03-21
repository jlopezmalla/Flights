package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait LocalSparkSession extends BeforeAndAfterAll { self: FlatSpec =>

  @transient implicit val session: SparkSession = LocalSparkSession.getNewLocalSparkSession(2, "test")
  @transient var sc: SparkContext = _


  override def beforeAll {
    Logger.getRootLogger.setLevel(Level.ERROR)
    sc = session.sparkContext
  }

  override def afterAll {
    System.clearProperty("spark.driver.port")
  }
}

object LocalSparkSession {

  def getNewLocalSparkSession(numExecutors: Int = 2, title: String): SparkSession =
    SparkSession.builder().config(new SparkConf()
      .setMaster(s"local[$numExecutors]")).appName(title).getOrCreate()

  private def getNewLocalContext(numExecutors: Int = 2, title: String): SparkContext = {
    new SparkContext(s"local[$numExecutors]", title)
  }
}
