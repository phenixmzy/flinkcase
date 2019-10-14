package com.flink.example.usecase

//import io.confluent.kafka.serializers.KafkaAvroSerializer
import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object ParamsAndPropertiesUtil {
  def loadKafkaCommonProperties(params: ParameterTool) = {
    val kafkaProperties = new Properties()
    // config consumer
    kafkaProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"524288000")
    kafkaProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"60000")
    kafkaProperties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"100000")
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, params.getRequired("group.id"))

    // config producer
    kafkaProperties.put(ProducerConfig.BATCH_SIZE_CONFIG,"200000")
    kafkaProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000")
    kafkaProperties.put(ProducerConfig.LINGER_MS_CONFIG, "100")
    kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    kafkaProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"524288000")
    kafkaProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,"134217728")
    kafkaProperties.put(ProducerConfig.SEND_BUFFER_CONFIG,"134217728")
    kafkaProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"134217728")

    /*
    Caused by: org.apache.kafka.common.errors.SerializationException: Can't convert value of class [B to class org.apache.kafka.common.serialization.StringSerializer specified in value.serializer
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    */

    kafkaProperties.put("bootstrap.servers", params.getRequired("bootstrap.servers"))

    kafkaProperties
  }
}
