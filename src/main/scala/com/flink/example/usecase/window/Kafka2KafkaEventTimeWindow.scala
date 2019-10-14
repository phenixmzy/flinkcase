package com.flink.example.usecase.window

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit.SECONDS

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.windowing.time.Time
import com.alibaba.fastjson.JSON
import com.flink.example.usecase.{CommonEnv, FlinkEnvUtil, ParamsAndPropertiesUtil}
import com.flink.example.usecase.assigner.GamePlayAssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

object Kafka2KafkaEventTimeWindow {

  def main(args: Array[String]) : Unit = {
    executor(args)
  }

  def executor(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val kafkaProperties = ParamsAndPropertiesUtil.loadKafkaCommonProperties(params)
    if (params.getNumberOfParameters < 6) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --bootstrap.servers <kafka brokers> --group.id <some id> " +
        "--zookeeper.connect <zk quorum> --output-topic <topic> --task-num <num> "
      )
      return
    }

    val inputTopic = params.getRequired("input-topic")
    val outputTopic = params.getRequired("output-topic")
    val isNoKey = params.getRequired("is-nokey")
    //val kafkaConsumer = new FlinkKafkaConsumer011(inputTopic, new SimpleStringSchema, params.getProperties)
    //val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, params.getProperties)
    //val sourceStream = env.addSource(kafkaConsumer)

    val env = CommonEnv.setEvn(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConsumer = new FlinkKafkaConsumer011(inputTopic, new SimpleStringSchema, kafkaProperties)
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, kafkaProperties)

    import org.apache.flink.api.scala._
    val sourceStream = env.addSource(kafkaConsumer)
    val gamePlayStream = sourceStream.map(jsonContent => {
      val json = JSON.parseObject(jsonContent)
      val gameId = json.getString("game_id")
      val startTime = json.getLongValue("start_time")
      val leaveTime = json.getLongValue("leave_time")
      val timeLen = leaveTime - startTime
      (gameId, timeLen.toInt, startTime, leaveTime)
    }).assignTimestampsAndWatermarks(new GamePlayAssignerWithPeriodicWatermarks()).map(item => (item._1, item._2))

    if (isNoKey.equals("nokey")) {
      gamePlayStream.timeWindowAll(Time.minutes(5))
        .reduce((value1, value2) => (value1._1, value1._2 + value2._2))
        .map(item => "noKey-"+item.toString())
        .addSink(kafkaProducer)
    } else {
      gamePlayStream.keyBy(0)
        .timeWindow(Time.of(300,SECONDS), Time.of(300, SECONDS))
        .reduce((value1, value2) => (value1._1, value1._2 + value2._2))
        .map(item => "keyBy-"+item.toString())
        .addSink(kafkaProducer)
    }
    env.execute()
  }
}