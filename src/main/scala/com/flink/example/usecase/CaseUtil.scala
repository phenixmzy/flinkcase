package com.flink.example.usecase

object CaseUtil {
  case class GamePlay(gameId : String, uid : String, startTimeStamp : Long, leaveTimeStamp : Long, timeLen : Int, userIp : String, gameType: String = "exe", channelFrom : String = "my", site : String = "kw") {
    def getGameId = gameId
    def getUserId = uid
    def getStartTimeStamp = startTimeStamp
    def getLeaveTimeStamp = leaveTimeStamp
    def getTimeLen = timeLen
    def getUserIp = userIp
    def getGameType = gameType
    def getChannelFrom = channelFrom
    def getSite = site
  }
}
