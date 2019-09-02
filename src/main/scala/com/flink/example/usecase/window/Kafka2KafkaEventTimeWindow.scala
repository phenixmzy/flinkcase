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
import com.flink.example.usecase.ParamsAndPropertiesUtil
import org.apache.flink.streaming.api.watermark.Watermark

object Kafka2KafkaEventTimeWindow {
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
    val isNoKey = params.getRequired("is-nokey")
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
      val leaveTime = json.getIntValue("leave_time")
      val timeLen = leaveTime - startTime
      (gameId, timeLen, startTime)
    }).assignTimestampsAndWatermarks(
      /**
        * watermarks的生成方式有两种
        * 1：With Periodic Watermarks：周期性的触发watermark的生成和发送
        * 2：With Punctuated Watermarks：基于某些事件触发watermark的生成和发送
        * */
      new AssignerWithPeriodicWatermarks[(String, Int, Int)]() {
      var currentMaxtTimestamp: Long = 0L
      private val maxOutOfOrderness = 3500L
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = new Watermark(currentMaxtTimestamp - maxOutOfOrderness)

      override def extractTimestamp(t: (String, Int, Int), l: Long): Long = {
        val timeStamp = t._3 * 1000
        currentMaxtTimestamp = Math.max(timeStamp.toLong, currentMaxtTimestamp)
        System.out.println("key:"+t._1+", eventtime:["+t._3+"|"+sdf.format(t._3 * 1000)+"], currentMaxTimestamp:["+currentMaxtTimestamp+"|"+
          sdf.format(currentMaxtTimestamp )+"], watermark:["+getCurrentWatermark().getTimestamp() + "|"+sdf.format(getCurrentWatermark().getTimestamp())+"]");
        timeStamp
      }
    }).map(item => (item._1, item._2))

    if (isNoKey.equals("nokey")) {
      gamePlayStream.timeWindowAll(Time.minutes(5), Time.minutes(5))
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