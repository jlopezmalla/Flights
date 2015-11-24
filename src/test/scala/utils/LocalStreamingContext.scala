/**
 * TODO: Put the license here!
 */

package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext, ClockWrapper}
import org.scalatest._
import org.scalatest.concurrent.Eventually

trait LocalSparkStreamingContext extends BeforeAndAfterAll with Eventually {
  self: FlatSpec =>

  @transient var sqc: SQLContext = _
  @transient var sc: SparkContext = _
  @transient var ssc: StreamingContext = _

  override def beforeAll {
    Logger.getRootLogger.setLevel(Level.WARN)

    System.setProperty("spark.cleaner.ttl", "300")
    System.setProperty("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    sqc = LocalSparkSqlContext.getNewLocalSqlContext(2, "test-Streaming")
    sc = sqc.sparkContext
    ssc = LocalSparkStreamingContext.getNewLocalStreamingContext(sc, 1)
  }

  lazy val clock = new ClockWrapper(ssc)

  override def afterAll {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    ssc.stop()
    ssc = null
    sc.stop()
    sc = null
  }
}

object LocalSparkStreamingContext {

  def getNewLocalStreamingContext(sparkContext: SparkContext, seconds: Int = 1): StreamingContext =
    new StreamingContext(sparkContext, Seconds(seconds))
}
