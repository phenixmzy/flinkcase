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

  def executor(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val kafkaProperties = ParamsAndPropertiesUtil.loadKafkaParamsAndProperties(params)

    if (params.getNumberOfParameters < 6) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --bootstrap.servers <kafka brokers> --group.id <some id> " +
        "--zookeeper.connect <zk quorum> --output-topic <topic> --task-num <num> "
      )
      return
    }

    val env = CommonEnv.setEvn(params)

    val inTopic = params.getRequired("input-topic")
    val outTopic = params.getRequired("out-topic")
    val kafkaConsumer = new FlinkKafkaConsumer011(inTopic, new SimpleStringSchema, kafkaProperties)
    val kafkaProducer = new FlinkKafkaProducer011(outTopic, new SimpleStringSchema, kafkaProperties)

    import org.apache.flink.api.scala._
    val etlSteam = env.addSource(kafkaConsumer)
      .map(jsonStr => {
        val json = JSON.parseObject(jsonStr)
        val gameId = json.getString("game_id")
        val gameType = json.getString("game_type")
        val userId = json.getString("user_id")
        val startTime = json.getLongValue("start_time")
        val leaveTime = json.getLongValue("leave_time")
        val timeLen = leaveTime - startTime
        val userIp = json.getString("user_ip")
        GamePlay(gameId, userId, startTime,leaveTime, timeLen.toInt, userIp)
      })

    val gamePlayCountStream = etlSteam
      .map(gamePlay => (gamePlay.gameId, 1)).keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce({ (v1, v2) => (v1._1, v1._2 + v2._2) })
      .map(item => item.toString())

    gamePlayCountStream.addSink(kafkaProducer)

    val gameAvgTimeStream = etlSteam.map(gamePlay => (gamePlay.gameId, gamePlay.timeLen)).keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .aggregate(new GameAvgTime)
      .map(kv => kv.toString)

    gameAvgTimeStream.addSink(kafkaProducer)

    /*val gameSummaryStream = etlSteam.map(gamePlay => (gamePlay.gameId, gamePlay.timeLen)).keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.minutes(10)))
        .aggregate(new GameSummary)

*/
    env.execute()
  }
}
