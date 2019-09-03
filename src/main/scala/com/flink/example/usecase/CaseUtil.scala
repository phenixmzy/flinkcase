package com.flink.example.usecase

object CaseUtil {
  case class GamePlay(gameId : String, uid : String, startTimeStamp : Int, leaveTimeStamp : Int, timeLen : Int, userIp : String)
}
