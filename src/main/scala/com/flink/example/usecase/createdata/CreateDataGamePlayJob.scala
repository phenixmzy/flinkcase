package com.flink.example.usecase.createdata

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.flink.example.usecase.source.GamePlaySource
import com.flink.example.usecase.{CommonEnv, ParamsAndPropertiesUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object CreateDataGamePlayJob {


  def main(args: Array[String]) : Unit = {
    executor(args)
  }

  def executor(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val kafkaProperties = ParamsAndPropertiesUtil.loadKafkaCommonProperties(params)
    if (params.getNumberOfParameters < 3) {
      println("Missing parameters!\n"
        + "Usage: Kafka --bootstrap.servers <kafka brokers> --output-topic <topic>  --data-times <num> "
      )
      println("params.getNumberOfParameters=" + params.getNumberOfParameters)
      return
    }
    val dataTimes = params.getRequired("data-times").toInt
    val outputTopic = params.getRequired("output-topic")
    val env = CommonEnv.setEvn(params)
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, kafkaProperties)
    import org.apache.flink.api.scala._
    val sourceStream = env.addSource(new GamePlaySource(dataTimes)).name("Make GamePlay Data").uid("Make GamePlay Data")
    val gamePlayStream = sourceStream.map(gamePlay => {
      JSON.toJSONString(gamePlay, SerializerFeature.WriteMapNullValue)
    }).name("Bean To Json Str").uid("Bean To Json Str").print()
    env.execute("Create Data GamePlay Job")
  }
}
