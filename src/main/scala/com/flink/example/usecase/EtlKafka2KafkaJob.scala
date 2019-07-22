package com.flink.example.usecase


import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.kafka.clients.consumer.ConsumerConfig

object EtlKafka2KafkaJob {
  val ONE_SECONDS = 1000L
  val ONE_MIN = 60 * ONE_SECONDS
  val CHECK_POINT_TIMEOUT = 10 * ONE_MIN


  def getParamsAndProperties(args:Array[String]): ParameterTool = {
    val params = ParameterTool.fromArgs(args)
    // config consumer
    params.getProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"524288000")
    params.getProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"60000")
    params.getProperties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"100000")

    // config producer
    params.getProperties.put(ProducerConfig.BATCH_SIZE_CONFIG,"200000")
    params.getProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000")
    params.getProperties.put(ProducerConfig.LINGER_MS_CONFIG, "100")
    params.getProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")

    params.getProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"524288000")
    params.getProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,"134217728")
    params.getProperties.put(ProducerConfig.SEND_BUFFER_CONFIG,"134217728")
    params.getProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"134217728")

    // config other
    params.getProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    params.getProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    params
  }

  def setEvn(params: ParameterTool): StreamExecutionEnvironment = {
    val taskNum = params.getRequired("task-num").toInt

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
    env.setBufferTimeout(100)
    env
  }

  def main(args: Array[String]) {
    executor(args)
  }

  def executor(args: Array[String]): Unit = {
    val params = getParamsAndProperties(args)
    if (params.getNumberOfParameters < 6) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --bootstrap.servers <kafka brokers> --group.id <some id> " +
        "--zookeeper.connect <zk quorum> --output-topic <topic> --task-num <num> "
      )
      return
    }
    val env = setEvn(params)

    val inputTopic = params.getRequired("input-topic")
    val outputTopic = params.getRequired("output-topic")
    val kafkaConsumer = new FlinkKafkaConsumer011(inputTopic, new SimpleStringSchema, params.getProperties)
    val kafkaProducer = new FlinkKafkaProducer011(outputTopic, new SimpleStringSchema, params.getProperties)
    val sourceStream = env.addSource(kafkaConsumer)
    val messageStream = sourceStream
      .filter(item => item != None)
      .flatMap(item => Some(item).get).map(item => item.toString)

    messageStream.addSink(kafkaProducer)

    // execute program
    env.execute("DataCollect-NG_PstSDK")
  }
}