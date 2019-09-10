package com.flink.example.usecase

import java.beans.Transient

import com.alibaba.fastjson.JSON
import com.flink.example.usecase.CaseUtil.GamePlay
import com.flink.example.usecase.TestRandom.{delayRand, playTimeLenRand}
import com.flink.example.usecase.source.GamePlaySource
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

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
    env.setBufferTimeout(100)
    env
  }

  def main(args: Array[String]) {
    executor(args)
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
    val outputTopic = params.getRequired("output-topic")
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, params.getProperties)

    val sourceStream = env.addSource(new GamePlaySource()).uid("create-data-gameplay")
    sourceStream.map(item => item.toString()).uid("gameplay-data-to-string").addSink(kafkaProducer)

    // execute program
    env.execute("DataCollect-GamePlay")
  }
}