package com.flink.example.usecase

object FlinkEnvUtil {
  val ONE_SECONDS = 1000L
  val ONE_MIN = 60 * ONE_SECONDS

  def getCheckPointInteravlMin(timeMin : Long) = {
    timeMin * ONE_MIN
  }

  def getCheckPointTimeOutMin(timeoutMin : Long) = {
    timeoutMin * ONE_MIN
  }
}
