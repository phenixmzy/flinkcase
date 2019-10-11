package com.flink.example.usecase.source

import com.flink.example.usecase.CaseUtil.GamePlay
import org.apache.flink.streaming.api.functions.source.SourceFunction

class GamePlaySource(val dataTimes: Int) extends SourceFunction[GamePlay] {
  val num = dataTimes;
  var isRunning:Boolean = true
  val gameTypes = Array[String]("exe", "web", "online", "flash")
  val channelFroms = Array[String]("my","category", "game_helper", "recommend", "762", "4399", "relateflash", "kuwo")
  val sites = Array[String]("index", "kw", "qvod", "baidu", "tx", "kugo")
  val ips = Array[String]("192.168.1.10","192.168.1.123","192.168.1.125","192.168.1.161","192.168.1.181","192.168.1.183","192.168.1.190","192.168.1.192","192.168.1.198","192.168.1.210")

  override def run(sourceContext: SourceFunction.SourceContext[GamePlay]): Unit = {
    while (isRunning) {
      Thread.sleep(10)
      for (carId <- 0 until num) {
        sourceContext.collect(getGamePlay)
      }
    }
  }

  override def cancel(): Unit = {
    isRunning = false
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
    val userIp = ips(getRandNum(0,10) % 10)
    GamePlay(gameId.toString, userId.toString, startTime, leaveTime,timeLen, userIp, gameType, channelFrom, site)
  }

  def getRandNum(min: Int, max: Int) = {
    (Math.random()*(max-min)+min).toInt
  }
}
