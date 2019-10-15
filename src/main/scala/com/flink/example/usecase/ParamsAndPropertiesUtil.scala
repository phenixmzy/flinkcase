package com.flink.example.usecase

import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object ParamsAndPropertiesUtil {
  def getKafakCommonProducerProperties(params: ParameterTool, bootstrapServer : String, isSetKVSerializer : Boolean = false, keySerializer : String = "org.apache.kafka.common.serialization.StringSerializer",  valueSerializer : String = "org.apache.kafka.common.serialization.StringSerializer") = {
    val kafkaProducerProperties = new Properties()
    // config producer
    kafkaProducerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000")
    kafkaProducerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG,"200000")
    kafkaProducerProperties.put(ProducerConfig.LINGER_MS_CONFIG, "100")
    kafkaProducerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    kafkaProducerProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"524288000")
    kafkaProducerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,"134217728")
    kafkaProducerProperties.put(ProducerConfig.SEND_BUFFER_CONFIG,"134217728")
    kafkaProducerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"134217728")
    if (isSetKVSerializer) {
      kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
      kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)
    }
    kafkaProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    kafkaProducerProperties
  }

  def getKafakCommonConsumerProperties(params: ParameterTool, bootstrapServer : String, isSetKVDeSerializer : Boolean = false, keyDeSerializer : String = "org.apache.kafka.common.serialization.StringDeserializer",  valueDeSerializer : String = "org.apache.kafka.common.serialization.StringDeserializer") = {
    val kafkaConsumerProperties = new Properties()
    // config producer
    kafkaConsumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"60000")
    kafkaConsumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"524288000")
    kafkaConsumerProperties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"100000")
    if (isSetKVDeSerializer) {
      kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerializer)
      kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerializer)
    }
    kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    kafkaConsumerProperties
  }

  def loadKafkaCommonProperties(params: ParameterTool) = {
    val kafkaProperties = new Properties()
    // config consumer
    kafkaProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"60000")
    kafkaProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"524288000")
    kafkaProperties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"100000")
    /*
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    */
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, params.getRequired("group.id"))

    // config producer
    kafkaProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000")
    kafkaProperties.put(ProducerConfig.BATCH_SIZE_CONFIG,"200000")
    kafkaProperties.put(ProducerConfig.LINGER_MS_CONFIG, "100")
    kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    kafkaProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"524288000")
    kafkaProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,"134217728")
    kafkaProperties.put(ProducerConfig.SEND_BUFFER_CONFIG,"134217728")
    kafkaProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"134217728")

    //Caused by: org.apache.kafka.common.errors.SerializationException: Can't convert value of class [B to class org.apache.kafka.common.serialization.StringSerializer specified in value.serializer
    /*
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    */

    kafkaProperties.put("bootstrap.servers", params.getRequired("bootstrap.servers"))

    kafkaProperties
  }
}
