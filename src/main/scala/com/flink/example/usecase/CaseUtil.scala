package com.flink.example.usecase

object CaseUtil {
  case class GamePlay(gameId : String, uid : String, gameType: String, startTimeStamp : Long, timeLen : Int, userIp : String)
}
