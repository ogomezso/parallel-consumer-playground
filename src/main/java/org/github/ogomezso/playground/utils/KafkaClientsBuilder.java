package org.github.ogomezso.playground.utils;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

@RequiredArgsConstructor
public class KafkaClientsBuilder<K,V> {

    public KafkaProducer<K, V> buildKafkaProducer(Properties kafkaConfig) {
        return new KafkaProducer<>(kafkaConfig);
    }

    public KafkaConsumer<K,V> buildKafkaConsumer(Properties kafkaConfig) {
        return new KafkaConsumer<>(kafkaConfig);
    }
}
