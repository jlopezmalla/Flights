package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

class ClockWrapper(ssc: StreamingContext) {

  private val manualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]

  def getTimeMillis: Long = manualClock.getTimeMillis()

  def setTime(timeToSet: Long) : Unit= manualClock.setTime(timeToSet)

  def advance(timeToAdd: Long) : Unit = manualClock.advance(timeToAdd)

  def waitTillTime(targetTime: Long): Long = manualClock.waitTillTime(targetTime)
}