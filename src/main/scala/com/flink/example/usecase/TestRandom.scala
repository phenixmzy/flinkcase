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
    /*for (i <- 1 to 100) {
      val v = gameIdRand.nextInt(100000)
      val currTimeStampMS = System.currentTimeMillis()
      val currTimeStamp = currTimeStampMS/1000
      val delay = delayRand.nextInt(300)
      val timeLen = playTimeLenRand.nextInt(300)
      val leaveTime = currTimeStamp - delay;
      val startTime = leaveTime - timeLen
      println("currTimeStampMS="+ currTimeStampMS + " currTimeStamp="+ currTimeStamp.toString + " delay=" + delay.toString + " leaveTime=" + leaveTime.toString)
      println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(leaveTime * 1000))
    }*/
    val logStr = "2019-09-24 17:54:33,129 INFO FSNamesystem.audit: allowed=true"
    println(getTimeStampMS(matchTime(logStr)))
  }

  def matchTime(log : String) = {
    import java.util.regex.Pattern
    val pattern = Pattern.compile("[0-9]{4}[-][0-9]{2}[-][0-9]{2}[ ][0-9]{2}[:][0-9]{2}[:][0-9]{2},[0-9]{3}")
    val matcher = pattern.matcher(log)
    if (matcher.find()) {
      matcher.group()
    } else {
      "非法数据"
    }
  }

  def getTimeStampMS(timeStr : String) = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
    df.parse(timeStr).getTime
  }
}
