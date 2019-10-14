package com.flink.example.usecase

//import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object ParamsAndPropertiesUtil {
  def loadKafkaParamsAndProperties(params: ParameterTool) = {
    // config consumer
    params.getProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"524288000")
    params.getProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"60000")
    params.getProperties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"100000")
    params.getProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, params.getRequired("bootstrap.servers"))
//    params.getProperties.put(ConsumerConfig.GROUP_ID_CONFIG, params.get("group.id"))

    // config producer
    params.getProperties.put(ProducerConfig.BATCH_SIZE_CONFIG,"200000")
    params.getProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000")
    params.getProperties.put(ProducerConfig.LINGER_MS_CONFIG, "100")
    params.getProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")

    params.getProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"524288000")
    params.getProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,"134217728")
    params.getProperties.put(ProducerConfig.SEND_BUFFER_CONFIG,"134217728")
    params.getProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"134217728")

    params.getProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    params.getProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    //params.getProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    //params.getProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])

    params.getProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params.getRequired("bootstrap.servers"))

    params.getProperties
  }
}
