package com.flink.example.usecase

import java.beans.Transient
import java.sql.Date
import java.text.SimpleDateFormat

import com.flink.example.usecase.CaseUtil.GamePlay
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

object TestRandom {
  val gameTypes = Array[String]("exe", "web", "online", "flash")
  val channelFroms = Array[String]("my","category", "game_helper", "recommend", "762", "4399", "relateflash", "kuwo")
  val sites = Array[String]("index", "kw", "qvod", "baidu", "tx", "kugo")
  val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
     var i = 0
    val gameTypes = Array[String]("exe", "web", "online", "flash")
    for (i <- 1 to 10000) {
      /*val gameId = ((Math.random()*9+1)*10000).toInt
      val userId = ((Math.random()*9+1)*10000000).toInt
      val currTimeStampMS = System.currentTimeMillis()
      val currTimeStamp = currTimeStampMS/1000
      val delay = delayRand.nextInt(300)
      val timeLen = playTimeLenRand.nextInt(300)
      val leaveTime = currTimeStamp - delay;
      val startTime = leaveTime - timeLen
      val leavelTimeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(leaveTime * 1000)
      println("gameId="+ gameId + " userId="+ userId + " currTimeStamp="+ currTimeStamp.toString + " delay=" + delay.toString  + " leaveTimeStr=" + leavelTimeStr + " leaveTime=" + leaveTime.toString)
*/
      println(JSON.toJSONString(getGamePlay, SerializerFeature.WriteMapNullValue))
    }
  }

  def getGamePlay() : GamePlay = {
    val gameId = ((Math.random()*9+1)*10000).toInt
    val userId = ((Math.random()*9+1)*10000000).toInt
    val currTimeStamp = System.currentTimeMillis()/1000
    val delay = getRandNum(1, 300)
    val timeLen = getRandNum(1, 300)
    val leaveTime = currTimeStamp - delay;
    val startTime = leaveTime - timeLen
    val gameType = gameTypes(getRandNum(0,4) % 4)
    val channelFrom = channelFroms(getRandNum(0,8) % 8)
    val site = sites(getRandNum(0,6) % 6)
    val gameplay = GamePlay(gameId.toString, userId.toString, startTime, leaveTime,timeLen, "127.0.0.1", gameType, channelFrom, site)

    println(gameplay.toString)
    gameplay
  }

  def getRandNum(min: Int, max: Int) = {
    (Math.random()*(max-min)+min).toInt
  }


}
