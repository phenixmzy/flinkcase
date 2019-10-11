package com.flink.example.usecase

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.scala.createTypeInformation

import scala.util.Random


object EtlKafka2KafkaJob {

  def executor(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    ParamsAndPropertiesUtil.loadKafkaParamsAndProperties(params)
    if (params.getNumberOfParameters < 3) {
      println("Missing parameters!\n"
        + "Usage: Kafka --bootstrap.servers <kafka brokers> --output-topic <topic> --task-num <num> "
      )
      return
    }

    val env = CommonEnv.setEvn(params)
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