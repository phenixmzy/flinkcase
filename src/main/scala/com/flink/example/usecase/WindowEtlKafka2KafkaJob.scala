package com.flink.example.usecase

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import com.alibaba.fastjson.JSON
import com.flink.example.usecase.CaseUtil.GamePlay
import com.flink.example.usecase.funcation.CaseFuncation.{GameAvgTime, GameSummary}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.functions.AggregateFunction


object WindowEtlKafka2KafkaJob {
  val ONE_SECONDS = 1000L
  val ONE_MIN = 60 * ONE_SECONDS
  val CHECK_POINT_TIMEOUT = 10 * ONE_MIN

  def setEvn(params: ParameterTool): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointTimeout(ONE_MIN * 3)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.enableCheckpointing(ONE_MIN)

    val taskNum = params.getRequired("task-num").toInt
    env.getConfig.setParallelism(taskNum)
    env.getConfig.setGlobalJobParameters(params)

    return env
  }

  def executor(array: Array[String]): Unit = {
    val params = ParamsAndPropertiesUtil.getKafkaParamsAndProperties(array)
    if (params.getNumberOfParameters < 6) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --bootstrap.servers <kafka brokers> --group.id <some id> " +
        "--zookeeper.connect <zk quorum> --output-topic <topic> --task-num <num> "
      )
      return
    }

    val env = setEvn(params)

    val inTopic = params.getRequired("input-topic")
    val outTopic = params.getRequired("out-topic")
    val kafkaConsumer = new FlinkKafkaConsumer011(inTopic, new SimpleStringSchema, params.getProperties)
    val kafkaProducer = new FlinkKafkaProducer011(outTopic, new SimpleStringSchema, params.getProperties)

    import org.apache.flink.api.scala._
    val etlSteam = env.addSource(kafkaConsumer)
      .map(jsonStr => {
        val json = JSON.parseObject(jsonStr)
        val gameId = json.getString("game_id")
        val gameType = json.getString("game_type")
        val userId = json.getString("user_id")
        val startTimeStamp = json.getIntValue("start_time")
        val levelTimeStamp = json.getIntValue("level_time")
        val timeLen = levelTimeStamp - startTimeStamp
        val userIp = json.getString("user_ip")
        GamePlay(gameId, userId, gameType, startTimeStamp, timeLen, userIp)
      })

    val gamePlayCountStream = etlSteam
      .map(gamePlay => (gamePlay.gameId, 1)).keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce({ (v1, v2) => (v1._1, v1._2 + v2._2) })
      .map(kv => {
        val outputStr = String.format("%s %s", kv._1, kv._2.toString)
        outputStr
      })
    gamePlayCountStream.addSink(kafkaProducer)

    val gameAvgTimeStream = etlSteam.map(gamePlay => (gamePlay.gameId, gamePlay.timeLen)).keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .aggregate(new GameAvgTime)
      .map(kv => String.format("%s", kv.toString))
    gameAvgTimeStream.addSink(kafkaProducer)

    /*val gameSummaryStream = etlSteam.map(gamePlay => (gamePlay.gameId, gamePlay.timeLen)).keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.minutes(10)))
        .aggregate(new GameSummary)

*/
    env.execute()
  }
}
