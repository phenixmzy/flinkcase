package com.flink.example.usecase.assigner

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * watermarks的生成方式有两种
  * 1：With Periodic Watermarks：周期性的触发watermark的生成和发送
  * 2：With Punctuated Watermarks：基于某些事件触发watermark的生成和发送
  * */
class GamePlayAssignerWithPeriodicWatermarks extends AssignerWithPeriodicWatermarks[(String, Int, Long, Long)]{
  var currentMaxtTimestamp : Long = 0L
  private val maxOutOfOrderness = 600*1000L
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  override def getCurrentWatermark: Watermark = new Watermark(currentMaxtTimestamp - maxOutOfOrderness)

  override def extractTimestamp(t: (String, Int, Long, Long), l: Long): Long = {
    val timeStamp = t._4 * 1000
    currentMaxtTimestamp = Math.max(timeStamp.toLong, currentMaxtTimestamp)
    timeStamp
  }
}
