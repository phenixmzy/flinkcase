package com.flink.example.usecase.window

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit.SECONDS

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.windowing.time.Time
import com.flink.example.usecase.{CommonEnv, FlinkEnvUtil, ParamsAndPropertiesUtil}
import com.flink.example.usecase.assigner.{GamePlayAssignerWithPeriodicWatermarks, GamePlayBeanAssignerWithPeriodicWatermarks}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.flink.example.usecase.CaseUtil.GamePlay
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.json4s._
import org.json4s.jackson.JsonMethods._

class GamePlayEventTimeWindow {
}

object GamePlayEventTimeWindow {
  val logger = LoggerFactory.getLogger(classOf[GamePlayEventTimeWindow])


  def main(args: Array[String]) : Unit = {
    executor(args)
  }

  def executor(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val kafkaProperties = ParamsAndPropertiesUtil.loadKafkaCommonProperties(params)
    logger.info("=========================== kafkaProperties.size:" + kafkaProperties.size())
    if (params.getNumberOfParameters < 5) {
      println("Missing parameters!\n"
        + "Usage: Kafka --bootstrap.servers <kafka brokers> --zookeeper.connect <zookeeper.connect> --input-topic <input-topic> --output-topic <output-topic> --task-num <num> --window-size <window-size>"
      )
      return
    }
    val inputTopic = params.getRequired("input-topic")
    val outputTopic = params.getRequired("output-topic")

    val env = CommonEnv.setEvn(params)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val taskNum = params.getRequired("task-num").toInt
    val windowSize = params.getRequired("window-size").toInt

    val kafkaConsumer = new FlinkKafkaConsumer011(inputTopic, new SimpleStringSchema, kafkaProperties)
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, kafkaProperties)

    import org.apache.flink.api.scala._
    implicit val formats = DefaultFormats
    val sourceStream = env.addSource(kafkaConsumer).name("kafka-source")
    val gamePlayStream = sourceStream.map(gamePlayJsonLine => {
      val gamePlay = parse(gamePlayJsonLine).extract[GamePlay]
      gamePlay
    }).map(gamePlay => {
      val map = Map("game_id" -> gamePlay.gameId, "gameplay_type" -> gamePlay.gameType, "user_id" -> gamePlay.uid)
      JSON.toJSONString(map, SerializerFeature.WriteMapNullValue)
    }).addSink(kafkaProducer).name("kafka-sink")


    /* .assignTimestampsAndWatermarks(new GamePlayBeanAssignerWithPeriodicWatermarks()).setParallelism(taskNum)
    val gamePlayWindowStream = gamePlayStream
      .keyBy(0)
      .map(gamePlay => (gamePlay.gameId, 1))
      // .timeWindow(Time.of(windowSize,SECONDS), Time.of(windowSize, SECONDS))
      .timeWindowAll(Time.of(windowSize,SECONDS), Time.of(windowSize, SECONDS))
      .reduce((g1, g2) => (g1._1, g1._2 + g2._2))
      .map(item => {
        val map = Map("game_id" -> item._1, "gameplay_count" -> item._2)
        JSON.toJSONString(map, SerializerFeature.WriteMapNullValue)
      }).addSink(kafkaProducer)
      */
    env.execute("game play event time window")
  }
}
