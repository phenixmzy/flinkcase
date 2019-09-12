package com.flink.example.usecase.window

import java.beans.Transient
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit.SECONDS

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.windowing.time.Time

import com.flink.example.usecase.ParamsAndPropertiesUtil
import com.flink.example.usecase.assigner.GamePlayAssignerWithPeriodicWatermarks
import com.flink.example.usecase.source.GamePlaySource

object GamePlayEventTimeWindow {
  val ONE_SECONDS = 1000L
  val ONE_MIN = 60 * ONE_SECONDS
  val CHECK_POINT_TIMEOUT = 10 * ONE_MIN

  def setEvn(params: ParameterTool): StreamExecutionEnvironment = {
    val taskNum = params.getRequired("task-num").toInt

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
      * 这个case使用事件时间.
      * */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.getConfig.disableSysoutLogging()
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))

    // create a checkpoint every 5 min
    env.enableCheckpointing(1 * ONE_MIN)
    env.getCheckpointConfig.setCheckpointTimeout(CHECK_POINT_TIMEOUT)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)
    env.setParallelism(taskNum)

    //Controlling Latency
    //env.setBufferTimeout(100)
    env
  }

  def main(args: Array[String]) : Unit = {
    executor(args)
  }

  def executor(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    ParamsAndPropertiesUtil.loadKafkaParamsAndProperties(params)
    if (params.getNumberOfParameters < 5) {
      println("Missing parameters!\n"
        + "Usage: Kafka --bootstrap.servers <kafka brokers> --output-topic <topic> --task-num <num> --window-size <window-size> --is-nokey <nokey>"
      )
      println("params.getNumberOfParameters=" + params.getNumberOfParameters)
      return
    }

    val outputTopic = params.getRequired("output-topic")
    val isNoKey = params.getRequired("is-nokey")

    val env = setEvn(params)
    val taskNum = params.getRequired("task-num").toInt
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, params.getProperties)
    val windowSize = params.getRequired("window-size").toInt
    import org.apache.flink.api.scala._
    val sourceStream = env.addSource(new GamePlaySource())
    val gamePlayStream = sourceStream.map(gamePlay => {
      val gameId = gamePlay.gameId
      val startTime = gamePlay.startTimeStamp
      val leaveTime = gamePlay.leaveTimeStamp
      val timeLen = gamePlay.timeLen
      (gameId, 1, startTime, leaveTime)
    }).assignTimestampsAndWatermarks(new GamePlayAssignerWithPeriodicWatermarks()).map(item => (item._1, item._2)).setParallelism(taskNum)

    if (isNoKey.equals("nokey")) {
      gamePlayStream.timeWindowAll(Time.of(windowSize,SECONDS), Time.of(windowSize, SECONDS))
        .reduce((value1, value2) => (value1._1, value1._2 + value2._2))
        .map(item => "noKey-"+item.toString())
        .addSink(kafkaProducer)
    } else {
      gamePlayStream.keyBy(0)
        .timeWindow(Time.of(windowSize,SECONDS), Time.of(windowSize, SECONDS))
        .reduce((value1, value2) => (value1._1, value1._2 + value2._2))
        .map(item => "keyBy-"+item.toString())
        .addSink(kafkaProducer)
    }
    env.execute()
  }
}
