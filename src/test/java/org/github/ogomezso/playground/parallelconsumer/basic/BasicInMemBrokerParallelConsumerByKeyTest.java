package org.github.ogomezso.playground.parallelconsumer.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.ogomezso.playground.parallelconsumer.TestRecordsMotherObject;
import org.github.ogomezso.playground.parallelconsumer.TopicsMotherObject;
import org.github.ogomezso.playground.utils.KafkaClientsBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.santander.libcom.kafka.test.clients.KafkaTestAdminClient;
import es.santander.libcom.kafka.test.clients.KafkaTestConsumer;
import es.santander.libcom.kafka.test.clients.KafkaTestProducer;
import es.santander.libcom.kafka.test.config.KafkaTestConfig;
import es.santander.libcom.kafka.test.objects.TestTopicConfig;
import es.santander.libcom.kafka.test.server.EmbeddedSingleNodeCluster;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicInMemBrokerParallelConsumerByKeyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private KafkaTestConfig adminConfig = new KafkaTestConfig("test-kafka-config.properties");
    private KafkaTestConfig producerConfig = new KafkaTestConfig("test-kafka-producer-config.properties");
    private KafkaTestConfig consumerConfig = new KafkaTestConfig("test-kafka-consumer-config.properties");
    private KafkaClientsBuilder<String, String> clients = new KafkaClientsBuilder<>();

    private final BasicParallelConsumerByKey class2Test = new BasicParallelConsumerByKey();

    public BasicInMemBrokerParallelConsumerByKeyTest() throws Exception {
    };

    @Test
    public void givenEmbeddedClusterWhenPollAndProduceThenAllRecordsAreProducedOnOutputTopic() throws Exception {

        EmbeddedSingleNodeCluster testCluster = new EmbeddedSingleNodeCluster(folder.newFolder("kafka"));
        testCluster.start();
        List<TestTopicConfig> testTopics = TopicsMotherObject.createTestTopics("in-memory-basic");
        adminConfig.setProperty("bootstrap.servers", testCluster.getBrokerConnectString());
        KafkaTestAdminClient testAdminClient = new KafkaTestAdminClient(adminConfig);
        testAdminClient.createTopics(testTopics);

        producerConfig.setProperty("bootstrap.servers", testCluster.getBrokerConnectString());
        producerConfig.setProperty("client.id", "in-memory-basic-producer");
        KafkaTestProducer<String, String> producer = new KafkaTestProducer<String, String>(producerConfig) {
            @Override
            public List<ProducerRecord<String, String>> processResult(
                    List<ProducerRecord<String, String>> producerRecords) {
                return producerRecords;
            }

            @Override
            public void handleError(Exception e, ProducerRecord<String, String> record) {
                throw (new RuntimeException("Error producing: " + record, e));

            }
        };

        consumerConfig.setProperty("bootstrap.servers", testCluster.getBrokerConnectString());
        consumerConfig.setProperty("client.id", "in-memory-basic-consumer");
        consumerConfig.setProperty("group.id", "in-memory-basic-test-cg");
        KafkaTestConsumer<String, String> consumer = new KafkaTestConsumer<String, String>(consumerConfig) {
            @Override
            public List<ConsumerRecord<String, String>> processRecords(List<ConsumerRecord<String, String>> records) {
                records.forEach(
                        record -> log.info("consumed record: {}, partition: {}", record.value(), record.partition()));
                return records;
            }
        };

        List<ProducerRecord<String, String>> actualProducedRecords = producer.produceMessages(testTopics.get(0).getTopicName(),
                TestRecordsMotherObject.createTestRecords());

        class2Test.runPollAndProduce(clients.buildKafkaConsumer(consumerConfig.getKafkaProperties()),
                clients.buildKafkaProducer(producerConfig.getKafkaProperties()),
                Collections.singletonList(testTopics.get(0).getTopicName()), testTopics.get(2).getTopicName());

        List<ConsumerRecord<String, String>> expectedConsumedRecords = consumer.pollOrTimeout(Duration.ofSeconds(10),
                20L, Collections.singletonList(testTopics.get(2).getTopicName()));

        assertEquals(expectedConsumedRecords.size(), actualProducedRecords.size());

        testCluster.shutdown();
    }
}