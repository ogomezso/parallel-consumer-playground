package org.github.ogomezso.playground.parallelconsumer.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.ogomezso.playground.parallelconsumer.TestRecordsMotherObject;
import org.github.ogomezso.playground.parallelconsumer.TopicsMotherObject;
import org.junit.Test;

import es.santander.libcom.kafka.test.clients.KafkaTestAdminClient;
import es.santander.libcom.kafka.test.clients.KafkaTestConsumer;
import es.santander.libcom.kafka.test.clients.KafkaTestProducer;
import es.santander.libcom.kafka.test.config.KafkaTestConfig;
import es.santander.libcom.kafka.test.objects.TestRecord;
import es.santander.libcom.kafka.test.objects.TestTopicConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicPCStreamConsumerTest {

    public BasicPCStreamConsumerTest() throws Exception {
    }

    private KafkaTestConfig streamConsumerTestAdminConfig = new KafkaTestConfig("test-kafka-config.properties");
    private KafkaTestConfig streamConsumerTestProducerConfig = new KafkaTestConfig(
            "test-kafka-producer-config.properties");
    private KafkaTestConfig streamsConsumerTestConsumerConfig = new KafkaTestConfig(
            "test-kafka-consumer-config.properties");

    private BasicPCStreamConsumer class2Test = new BasicPCStreamConsumer();

    @Test
    public void givenStreamFolderCreated() throws Exception {

        List<TestTopicConfig> testTopics = TopicsMotherObject.createTestTopics("real-stream-test");
        List<TestRecord<String, String>> testRecords = TestRecordsMotherObject.createWordcountRecords();

        KafkaTestAdminClient testAdminClient = new KafkaTestAdminClient(streamConsumerTestAdminConfig);
        testAdminClient.createTopics(testTopics);

        String appId = "testApp";
        class2Test.createStream(testTopics.get(0).getTopicName(), testTopics.get(1).getTopicName(), "temp",
                appId);

        streamConsumerTestProducerConfig.setProperty("client.id", "real-streams-test-producer");
        KafkaTestProducer<String, String> realClusterProducer = new KafkaTestProducer<String, String>(
                streamConsumerTestProducerConfig) {
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

        realClusterProducer
                .produceMessages(testTopics.get(0).getTopicName(), testRecords);

        streamsConsumerTestConsumerConfig.setProperty("client.id", "real-streams-test-consumer");
        streamsConsumerTestConsumerConfig.setProperty("group.id", "real-streams-test-cg");
        KafkaTestConsumer<String, String> consumer = new KafkaTestConsumer<String, String>(
                streamsConsumerTestConsumerConfig) {
            @Override
            public List<ConsumerRecord<String, String>> processRecords(List<ConsumerRecord<String, String>> records) {
                records.forEach(
                        record -> log.info("consumed record: {}, partition: {}", record.value(), record.partition()));
                return records;
            }
        };
        List<ConsumerRecord<String, String>> actualRecords = consumer.pollOrTimeout(Duration.ofSeconds(60L), 50L,
                List.of(testTopics.get(1).getTopicName()));

        class2Test.close();
        class2Test.cleanup();
        List<String> topicToDelete = testTopics.stream().map(topicConfig -> topicConfig.getTopicName())
                .collect(Collectors.toList());
        testAdminClient.deleteTopics(topicToDelete);

        assertEquals(6, actualRecords.size());
    }
}
