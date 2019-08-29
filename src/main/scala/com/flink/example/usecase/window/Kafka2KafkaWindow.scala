package com.flink.example.usecase.window

import java.util.concurrent.TimeUnit.SECONDS

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.windowing.time.Time
import com.alibaba.fastjson.JSON
import com.flink.example.usecase.ParamsAndPropertiesUtil


object Kafka2KafkaWindow {
  val ONE_SECONDS = 1000L
  val ONE_MIN = 60 * ONE_SECONDS
  val CHECK_POINT_TIMEOUT = 10 * ONE_MIN

  def setEvn(params: ParameterTool): StreamExecutionEnvironment = {
    val taskNum = params.getRequired("task-num").toInt

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

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
    val params = ParamsAndPropertiesUtil.getKafkaParamsAndProperties(args)
    if (params.getNumberOfParameters < 6) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --bootstrap.servers <kafka brokers> --group.id <some id> " +
        "--zookeeper.connect <zk quorum> --output-topic <topic> --task-num <num> "
      )
      return
    }


    val inputTopic = params.getRequired("input-topic")
    val outputTopic = params.getRequired("output-topic")
    //val kafkaConsumer = new FlinkKafkaConsumer011(inputTopic, new SimpleStringSchema, params.getProperties)
    //val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, params.getProperties)
    //val sourceStream = env.addSource(kafkaConsumer)

    val env = setEvn(params)
    val kafkaConsumer = new FlinkKafkaConsumer011(inputTopic, new SimpleStringSchema, params.getProperties)
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, params.getProperties)

    import org.apache.flink.api.scala._
    val sourceStream = env.addSource(kafkaConsumer)
    val gamePlayStream = sourceStream.map(jsonContent => {
      val json = JSON.parseObject(jsonContent)
      val gameId = json.getString("game_id")
      val startTime = json.getIntValue("start_time")
      val end_time = json.getIntValue("end_time")
      val timeLen = end_time - startTime
      (gameId, timeLen)
    })

    gamePlayStream.keyBy(0)
      .timeWindow(Time.of(300,SECONDS), Time.of(300, SECONDS))
      .reduce((value1, value2) => (value1._1, value1._2 + value2._2))
      .map(item => item.toString())
      .addSink(kafkaProducer)

    env.execute()
  }
}
