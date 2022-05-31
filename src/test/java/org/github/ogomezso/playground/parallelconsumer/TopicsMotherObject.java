package org.github.ogomezso.playground.parallelconsumer;

import java.util.Arrays;
import java.util.List;

import es.santander.libcom.kafka.test.objects.TestTopicConfig;


public class TopicsMotherObject {

    public static List<TestTopicConfig> createTestTopics(String namePrefix) {
        return Arrays.asList(
                TestTopicConfig.builder()
                        .partitionNumber(1)
                        .replicationFactor((short) 3)
                        .topicName(namePrefix + "-input-topic-1")
                        .build(),
                TestTopicConfig.builder()
                        .partitionNumber(1)
                        .replicationFactor((short) 3)
                        .topicName(namePrefix + "-input-topic-2")
                        .build(),
                TestTopicConfig.builder()
                        .partitionNumber(3)
                        .replicationFactor((short) 3)
                        .topicName(namePrefix + "-output-topic")
                        .build()

        );
    }

}
