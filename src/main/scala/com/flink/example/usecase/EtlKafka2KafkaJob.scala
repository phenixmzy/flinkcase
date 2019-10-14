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
    val kafkaProperties = ParamsAndPropertiesUtil.loadKafkaParamsAndProperties(params)

    if (params.getNumberOfParameters < 3) {
      println("Missing parameters!\n"
        + "Usage: Kafka --bootstrap.servers <kafka brokers> --output-topic <topic> --task-num <num> "
      )
      return
    }

    val env = CommonEnv.setEvn(params)
    val inputTopic = params.getRequired("input-topic")
    val outputTopic = params.getRequired("output-topic")
    val kafkaConsumer = new FlinkKafkaConsumer011(inputTopic, new SimpleStringSchema, kafkaProperties)
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, kafkaProperties)

    val sourceStream = env.addSource(kafkaConsumer)
                        .addSink(kafkaProducer)
    // execute program
    env.execute("DataCollect-GamePlay")
  }
}