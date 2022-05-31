package org.github.ogomezso.playground.parallelconsumer.streams;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class BasicPCStreamConsumer {

    KafkaStreams streams;

    private Properties createStreamsConfigProperties(String applicationId, String stateDir) {

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "35.197.207.46:9092");
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        streamsConfiguration.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        streamsConfiguration.put(
                StreamsConfig.STATE_DIR_CONFIG, stateDir);

        return streamsConfiguration;
    }

    private void createWordCountStream(final StreamsBuilder builder, String inputTopic, String outPutTopic) {

        KStream<String, String> textLines = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        KTable<String, Long> wordCounts = textLines
        .flatMapValues(value -> List.of(pattern.split(value.toLowerCase())))
        .groupBy((keyIgnored, word) -> word)
        .count();

        KStream<String, Long> result= wordCounts.toStream();
        
        result.print(Printed.toSysOut());
        result.to(outPutTopic, Produced.with(Serdes.String(), Serdes.Long()));
    }
 
    public void createStream(String inputTopic, String outputTopic, String stateDir, String applicationId) {
        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder, inputTopic, outputTopic);
        streams = new KafkaStreams(builder.build(),
                createStreamsConfigProperties(applicationId, stateDir));
        streams.start();
    }

    public void close() {
        streams.close();
    }

    public void cleanup() {
        streams.cleanUp();
    }
}
