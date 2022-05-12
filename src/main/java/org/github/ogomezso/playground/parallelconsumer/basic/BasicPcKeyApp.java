package org.github.ogomezso.playground.parallelconsumer.basic;

import org.github.ogomezso.playground.utils.KafkaClientsBuilder;
import org.github.ogomezso.playground.utils.KafkaConfigBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class BasicPcKeyApp {

    public static void main(String[] args) throws IOException {

        KafkaConfigBuilder producerConfig = new KafkaConfigBuilder("kafka-producer-config.properties");
        KafkaConfigBuilder consumerConfig = new KafkaConfigBuilder("kafka-consumer-config.properties");
        KafkaClientsBuilder<String, String> clients = new KafkaClientsBuilder<>();
        List<String> inputTopic = Collections.singletonList("input-topic-pc-basic-key");
        BasicParallelConsumerByKey pc = new BasicParallelConsumerByKey();

        pc.run(clients.buildKafkaConsumer(consumerConfig.getKafkaProperties()), clients.buildKafkaProducer(producerConfig.getKafkaProperties()), inputTopic);
    }

}
