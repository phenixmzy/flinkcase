package com.flink.example.usecase

import java.beans.Transient
import java.sql.Date
import java.text.SimpleDateFormat

import scala.util.Random

object TestRandom {
  val gameIdRand = new Random()
  val userIdRand = new Random()
  val delayRand = new Random()
  val playTimeLenRand = new Random()
  val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
     var i = 0
    for (i <- 1 to 100000) {
      val v = gameIdRand.nextInt(100000)
      val currTimeStamp = System.currentTimeMillis()/1000
      val delay = delayRand.nextInt(300)
      val timeLen = playTimeLenRand.nextInt(300)
      val leaveTime = currTimeStamp - delay;
      val startTime = leaveTime - timeLen
      println("currTimeStamp="+ currTimeStamp.toString + " delay=" + delay.toString + " leaveTime=" + leaveTime.toString)
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(leaveTime * 1000))
    }

  }
}
