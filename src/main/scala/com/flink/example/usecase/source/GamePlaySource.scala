package com.flink.example.usecase.source

import java.beans.Transient

import com.flink.example.usecase.CaseUtil.GamePlay
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class GamePlaySource extends SourceFunction[GamePlay]{
  val num = 1000;
  var isRunning:Boolean = true

  @Transient lazy val gameIdRand = new Random()
  @Transient lazy val userIdRand = new Random()
  @Transient lazy val delayRand = new Random()
  @Transient lazy val playTimeLenRand = new Random()

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
    val gameId = gameIdRand.nextInt(100000).toString
    val userId = userIdRand.nextInt(10000000).toString
    val currTimeStamp = System.currentTimeMillis()/1000
    val delay = delayRand.nextInt(300)
    val timeLen = playTimeLenRand.nextInt(300)
    val leaveTime = currTimeStamp - delay;
    val startTime = leaveTime - timeLen
    GamePlay(gameId, userId, startTime, leaveTime,timeLen, "127.0.0.1")
  }
}
