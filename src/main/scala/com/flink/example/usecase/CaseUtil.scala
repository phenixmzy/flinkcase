package com.flink.example.usecase

object CaseUtil {
  case class GamePlay(gameId : String, uid : String, startTimeStamp : Long, leaveTimeStamp : Long, timeLen : Int, userIp : String)
}
