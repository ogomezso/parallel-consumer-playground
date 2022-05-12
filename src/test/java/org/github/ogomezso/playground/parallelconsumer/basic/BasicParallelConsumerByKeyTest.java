package org.github.ogomezso.playground.parallelconsumer.basic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.ogomezso.playground.utils.KafkaClientsBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import es.santander.kafka.test.clients.KafkaTestAdminClient;
import es.santander.kafka.test.clients.KafkaTestConsumer;
import es.santander.kafka.test.clients.KafkaTestProducer;
import es.santander.kafka.test.config.KafkaTestConfig;
import es.santander.kafka.test.objects.TestRecord;
import es.santander.kafka.test.objects.TestTopicConfig;
import es.santander.kafka.test.server.EmbeddedSingleNodeCluster;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BasicParallelConsumerByKeyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private KafkaTestConfig adminConfig = new KafkaTestConfig("test-kafka-config.properties");
    private KafkaTestConfig producerConfig = new KafkaTestConfig("test-kafka-producer-config.properties");
    private KafkaTestConfig consumerConfig = new KafkaTestConfig("test-kafka-consumer-config.properties");
    private KafkaClientsBuilder<String, String> clients = new KafkaClientsBuilder<>();

    private final BasicParallelConsumerByKey class2Test = new BasicParallelConsumerByKey();

    public BasicParallelConsumerByKeyTest() throws Exception {
    };

    @Test
    public void givenTemporaryFolderEmbeddedClusterShouldStart() throws Exception {

        EmbeddedSingleNodeCluster testCluster = new EmbeddedSingleNodeCluster(folder.newFolder("kafka"));
        testCluster.start();
        List<TestTopicConfig> test_topics = createTestTopics();
        adminConfig.setProperty("bootstrap.servers", testCluster.getBrokerConnectString());
        KafkaTestAdminClient testAdminClient = new KafkaTestAdminClient(adminConfig);
        testAdminClient.createTopics(test_topics);

        producerConfig.setProperty("bootstrap.servers", testCluster.getBrokerConnectString());
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
        KafkaTestConsumer<String, String> consumer = new KafkaTestConsumer<String, String>(consumerConfig) {
            @Override
            public List<ConsumerRecord<String, String>> processRecords(List<ConsumerRecord<String, String>> records) {
                records.forEach(record -> log.info("record consumido: {}, partition: {}", record.value(), record.partition()));
                return records;
            }
        };

        List<ProducerRecord<String, String>> actualProducedRecords = producer.produceMessages("input-topic-1",
                createTestRecords());

        class2Test.runPollAndProduce(clients.buildKafkaConsumer(consumerConfig.getKafkaProperties()),
                clients.buildKafkaProducer(producerConfig.getKafkaProperties()), Collections.singletonList("input-topic-1"), "output-topic");

        List<ConsumerRecord<String,String>> expectedConsumedRecords =  consumer.pollOrTimeout(Duration.ofSeconds(10), 20L, Collections.singletonList("output-topic"));

        assertEquals(expectedConsumedRecords.size(), actualProducedRecords.size());
        
        testCluster.shutdown();
    }

    private List<TestRecord<String, String>> createTestRecords() {
        return Arrays.asList(
                TestRecord.<String, String>builder().key("1").value("1.b").build(),
                TestRecord.<String, String>builder().key("1").value("1.a").build(),
                TestRecord.<String, String>builder().key("1").value("1.c").build(),
                TestRecord.<String, String>builder().key("1").value("1.d").build(),
                TestRecord.<String, String>builder().key("1").value("1.e").build(),
                TestRecord.<String, String>builder().key("2").value("2.a").build(),
                TestRecord.<String, String>builder().key("2").value("2.b").build(),
                TestRecord.<String, String>builder().key("2").value("2.c").build(),
                TestRecord.<String, String>builder().key("2").value("2.d").build(),
                TestRecord.<String, String>builder().key("2").value("2.e").build(),
                TestRecord.<String, String>builder().key("3").value("3.a").build(),
                TestRecord.<String, String>builder().key("3").value("3.b").build(),
                TestRecord.<String, String>builder().key("3").value("3.c").build(),
                TestRecord.<String, String>builder().key("3").value("3.d").build(),
                TestRecord.<String, String>builder().key("3").value("3.e").build(),
                TestRecord.<String, String>builder().key("4").value("4.a").build(),
                TestRecord.<String, String>builder().key("4").value("4.b").build(),
                TestRecord.<String, String>builder().key("4").value("4.c").build(),
                TestRecord.<String, String>builder().key("4").value("4.d").build(),
                TestRecord.<String, String>builder().key("4").value("4.e").build());
    }

    private List<TestTopicConfig> createTestTopics() {
        return Arrays.asList(
                TestTopicConfig.builder()
                        .partitionNumber(1)
                        .replicationFactor((short) 1)
                        .topicName("input-topic-1")
                        .build(),
                TestTopicConfig.builder()
                        .partitionNumber(1)
                        .replicationFactor((short) 1)
                        .topicName("input-topic-2")
                        .build(),
                TestTopicConfig.builder()
                        .partitionNumber(3)
                        .replicationFactor((short) 1)
                        .topicName("output-topic")
                        .build()

        );
    }
}