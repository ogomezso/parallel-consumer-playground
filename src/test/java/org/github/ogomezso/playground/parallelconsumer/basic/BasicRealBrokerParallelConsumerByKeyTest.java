package org.github.ogomezso.playground.parallelconsumer.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.github.ogomezso.playground.parallelconsumer.TestRecordsMotherObject;
import org.github.ogomezso.playground.parallelconsumer.TopicsMotherObject;
import org.github.ogomezso.playground.utils.KafkaClientsBuilder;
import org.junit.Test;

import es.santander.kafka.test.clients.KafkaTestAdminClient;
import es.santander.kafka.test.clients.KafkaTestProducer;
import es.santander.kafka.test.config.KafkaTestConfig;
import es.santander.kafka.test.objects.TestRecord;
import es.santander.kafka.test.objects.TestTopicConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicRealBrokerParallelConsumerByKeyTest {

    private KafkaTestConfig RealConsumerTestAdminConfig = new KafkaTestConfig("test-kafka-config.properties");
    private KafkaTestConfig RealConsumerTestProducerConfig = new KafkaTestConfig(
            "test-kafka-producer-config.properties");
    private KafkaTestConfig RealConsumerTestConsumerConfig = new KafkaTestConfig(
            "test-kafka-consumer-config.properties");
    private KafkaClientsBuilder<String, String> clients = new KafkaClientsBuilder<>();

    private final BasicParallelConsumerByKey class2Test = new BasicParallelConsumerByKey();

    public BasicRealBrokerParallelConsumerByKeyTest() throws Exception {
    };

    @Test
    public void givenRealClusterWhenPollThenAllRecordsAreConsumed() throws Exception {

        List<TestTopicConfig> testTopics = TopicsMotherObject.createTestTopics("real-consumer-test");
        List<TestRecord<String, String>> testRecords = TestRecordsMotherObject.createTestRecords();

        KafkaTestAdminClient testAdminClient = new KafkaTestAdminClient(RealConsumerTestAdminConfig);
        testAdminClient.createTopics(testTopics);

        RealConsumerTestProducerConfig.setProperty("client.id", "real-basic-consumer-test-producer");
        KafkaTestProducer<String, String> realClusterProducer = new KafkaTestProducer<String, String>(
                RealConsumerTestProducerConfig) {
            @Override
            public List<ProducerRecord<String, String>> processResult(
                    List<ProducerRecord<String, String>> producerRecords) {
                producerRecords.forEach(record -> log.info("record produced: {}", record));
                return producerRecords;
            }

            @Override
            public void handleError(Exception e, ProducerRecord<String, String> record) {
                throw (new RuntimeException("Error producing: " + record, e));

            }
        };

        realClusterProducer.produceMessages(testTopics.get(0).getTopicName(), testRecords);

        RealConsumerTestConsumerConfig.setProperty("client.id", "real-pc-basic-consumer-test");
        RealConsumerTestConsumerConfig.setProperty("group.id", "real-pc-basic-consumer-test-cg");

        class2Test.poll(clients.buildKafkaConsumer(RealConsumerTestConsumerConfig.getKafkaProperties()),
                clients.buildKafkaProducer(RealConsumerTestProducerConfig.getKafkaProperties()),
                Collections.singletonList(testTopics.get(0).getTopicName()));

        Awaitility.await().untilAsserted(() -> assertEquals(20, class2Test.consumerCount.get()));

        List<String> topicToDelete = testTopics.stream().map(topicConfig -> topicConfig.getTopicName())
                .collect(Collectors.toList());
        testAdminClient.deleteTopics(topicToDelete);
    }
}
