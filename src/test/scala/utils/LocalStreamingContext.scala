package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

trait LocalSparkStreamingContext extends BeforeAndAfterAll { self: FlatSpec =>

  @transient var sqc: SQLContext = _
  @transient var sc: SparkContext = _
  @transient var ssc: StreamingContext = _

  override def beforeAll {
    Logger.getRootLogger.setLevel(Level.WARN)
    sqc = LocalSparkSqlContext.getNewLocalSqlContext(2, "test-Streaming")
    sc = sqc.sparkContext
    ssc = LocalSparkStreamingContext.getNewLocalStreamingContext(sc, 1)
  }

  override def afterAll {
    sqc.sparkContext.stop()
    ssc.stop(false, true)
    System.clearProperty("spark.driver.port")
  }
}

object LocalSparkStreamingContext {

  def getNewLocalStreamingContext(sparkContext: SparkContext, seconds: Int = 1): StreamingContext =
    new StreamingContext(sparkContext, Seconds(seconds))
}
