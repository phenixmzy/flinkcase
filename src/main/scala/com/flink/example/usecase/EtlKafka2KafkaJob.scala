package com.flink.example.usecase

import java.beans.Transient

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.CheckpointingMode

import scala.util.Random
import scala.util.parsing.json.JSONObject


object EtlKafka2KafkaJob {
  val ONE_SECONDS = 1000L
  val ONE_MIN = 60 * ONE_SECONDS
  val CHECK_POINT_TIMEOUT = 10 * ONE_MIN


  def setEvn(params: ParameterTool): StreamExecutionEnvironment = {
    val taskNum = params.getRequired("task-num").toInt

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

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

  def executor(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    ParamsAndPropertiesUtil.loadKafkaParamsAndProperties(params)
    if (params.getNumberOfParameters < 3) {
      println("Missing parameters!\n"
        + "Usage: Kafka --bootstrap.servers <kafka brokers> --output-topic <topic> --task-num <num> "
      )
      return
    }
    val env = setEvn(params)
    val inputTopic = params.getRequired("input-topic")
    val outputTopic = params.getRequired("output-topic")
    val kafkaConsumer = new FlinkKafkaConsumer011(inputTopic, new SimpleStringSchema, params.getProperties)
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, params.getProperties)

    val sourceStream = env.addSource(kafkaConsumer).uid("consumer-gameplay-data-from-kafka")
    sourceStream.map(jsonStr => {
      val delayRand = new Random()
      val playTimeLenRand = new Random()
      val currTimeStamp = System.currentTimeMillis()/1000
      val delay = delayRand.nextInt(300)
      val timeLen = playTimeLenRand.nextInt(300)

      val obj = JSON.parseObject(jsonStr)
      val gameSleep = obj.getInteger("sleep")
      val leaveTime = currTimeStamp - delay
      val startTime = leaveTime - timeLen - gameSleep
      val channel = obj.getString("from")
      obj.put("start_time", startTime)
      obj.put("leave_time", leaveTime)
      obj.put("game_exp", timeLen)
      obj.put("from_channel", channel)
      obj.remove("from")

      obj.toJSONString
    }).uid("create-game-play-data").addSink(kafkaProducer)

    // execute program
    env.execute("DataCollect-GamePlay")
  }
}