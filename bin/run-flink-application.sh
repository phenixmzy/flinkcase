#!/usr/bin/env bash
/usr/local/flink/bin/flink run \
        -m yarn-cluster \
        -ys 3 \
        -yjm 2048m \
        -ytm 3096m \
        -yn 4 \
        -d \
        -c com.flink.example.usecase.EtlKafka2KafkaJob \
        /root/flinkcase/gameplay-eventtime-window.jar \
        --output-topic gameplay  \
        --task-num 9 \
        --bootstrap.servers yzj-client-01:9092,yzj-client-02:9092,yzj-client-03:9092 \
        --group.id test-flink-kafka-gid