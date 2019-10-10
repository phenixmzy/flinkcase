package com.flink.example.usecase.window

import java.util.concurrent.TimeUnit.SECONDS

import com.flink.example.usecase.parse.log.ParseLogUtil
import com.flink.example.usecase.window.GamePlayEventTimeWindow.setEvn
import com.flink.example.usecase.{FlinkEnvUtil, ParamsAndPropertiesUtil}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object HDFSAuditEventTimeWindow {

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
    env.enableCheckpointing(FlinkEnvUtil.getCheckPointInteravlMin(1))
    env.getCheckpointConfig.setCheckpointTimeout(FlinkEnvUtil.getCheckPointTimeOutMin(10))
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
    if (params.getNumberOfParameters < 5) {
      println("Missing parameters!\n"
        + "Usage: Kafka --bootstrap.servers <kafka brokers> --input-topic <input-topic> --output-topic <output-topic> --task-num <num> --window-size <window-size>"
      )
      return
    }

    val inputTopic = params.getRequired("input-topic")
    val outputTopic = params.getRequired("output-topic")
    val taskNum = params.getRequired("task-num").toInt

    val env = setEvn(params)

    val kafkaConsumer = new FlinkKafkaConsumer011(inputTopic, new SimpleStringSchema, params.getProperties)
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, params.getProperties)
    val windowSize = params.getRequired("window-size").toInt
    import org.apache.flink.api.scala._
    val hdfsAuditStream = env.addSource(kafkaConsumer)
      .map(line => ParseLogUtil.parseHDFSAuditLog(line)).map(item => (item._5,1))
      .timeWindowAll(Time.of(windowSize,SECONDS), Time.of(windowSize, SECONDS))
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
      .map(item => item.toString())
      .addSink(kafkaProducer)
  }

  def main(args: Array[String]): Unit = {
    executor(args)
  }
}
